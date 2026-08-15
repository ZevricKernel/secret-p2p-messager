#!/usr/bin/env python3
"""
P2P Messenger — End-to-end encrypted peer-to-peer chat.

Flow:
  1. Both clients register with the signaling server (Cloudflare Worker).
  2. They exchange WebRTC SDP offers/answers + ICE candidates via the server.
  3. Once the DTLS handshake completes, ALL chat flows directly
     between the two peers. The server is no longer involved.

Security:
  - WebRTC DataChannels use mandatory DTLS encryption.
  - The signaling server only sees SDP/ICE metadata — never chat text.
  - No messages are stored anywhere after delivery.
"""

import asyncio
import json
import sys
import time
from datetime import datetime

import aiohttp
from aiortc import (
    RTCPeerConnection,
    RTCSessionDescription,
    RTCConfiguration,
    RTCIceServer,
)

# ── Windows event loop fix (aiortc requires SelectorEventLoop) ──
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

# ── ANSI terminal colors ────────────────────────────────────────
CYAN   = "\033[96m"
GREEN  = "\033[92m"
YELLOW = "\033[93m"
RED    = "\033[91m"
MAGENTA= "\033[95m"
DIM    = "\033[2m"
BOLD   = "\033[1m"
RESET  = "\033[0m"


def timestamp():
    return datetime.now().strftime("%H:%M:%S")


def sys_msg(text):
    print(f"{DIM}{timestamp()} [SYS] {text}{RESET}")


def banner():
    print(f"""
{BOLD}{CYAN}╔═══════════════════════════════════════════════╗
║          P2P Messenger  ·  WebRTC            ║
║   End-to-end encrypted · No server relay     ║
╚═══════════════════════════════════════════════╝{RESET}
""")


# ═══════════════════════════════════════════════════════════════
class Messenger:
    """Handles signaling, WebRTC connection, and the chat loop."""

    def __init__(self, signal_url: str, room: str, username: str):
        self.signal_url = signal_url.rstrip("/")
        self.room = room
        self.username = username
        self.peer_name: str | None = None
        self.is_initiator = False
        self.pc: RTCPeerConnection | None = None
        self.channel = None
        self.candidate_buffer: list[dict] = []
        self.running = False
        self._connected = asyncio.Event()

    # ── Signaling API helpers ─────────────────────────────────

    async def _api(self, endpoint: str, data: dict) -> dict:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{self.signal_url}/{endpoint}",
                json=data,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                return await resp.json()

    async def join_room(self) -> list[str]:
        result = await self._api("join", {
            "room": self.room,
            "username": self.username,
        })
        return result.get("members", [])

    async def send_signal(self, sig_type: str, payload: dict):
        await self._api("send", {
            "room": self.room,
            "from": self.username,
            "to": self.peer_name,
            "type": sig_type,
            "payload": payload,
        })

    async def poll_signals(self) -> list[dict]:
        result = await self._api("poll", {
            "room": self.room,
            "username": self.username,
        })
        return result.get("signals", [])

    async def leave_room(self):
        try:
            await self._api("leave", {
                "room": self.room,
                "username": self.username,
            })
        except Exception:
            pass

    # ── DataChannel setup ─────────────────────────────────────

    def _wire_channel(self, channel):
        """Attach event handlers to a DataChannel."""

        @channel.on("message")
        def on_message(message):
            # Print incoming message, then re-display the prompt
            print(
                f"\r{MAGENTA}{timestamp()} [{self.peer_name}] {message}{RESET}\n"
                f"{GREEN}> {RESET}",
                end="",
                flush=True,
            )

        @channel.on("close")
        def on_close():
            sys_msg("Peer disconnected.")
            self.running = False

    # ── Wait for a peer to appear in the room ────────────────

    async def _wait_for_peer(self):
        sys_msg(f"Joined room '{self.room}' as '{self.username}'. Waiting for peer…")
        while True:
            members = await self.join_room()
            others = [m for m in members if m != self.username]
            if others:
                self.peer_name = others[0]
                # First one to join becomes the initiator
                self.is_initiator = (
                    members.index(self.username) < members.index(self.peer_name)
                )
                role = "initiator" if self.is_initiator else "responder"
                sys_msg(f"Peer '{self.peer_name}' found — you are the {role}.")
                return
            await asyncio.sleep(1.5)

    # ── Signal exchange loop (runs in background) ─────────────

    async def _signal_loop(self):
        """Poll the signaling server and feed messages into WebRTC."""
        while self.running:
            try:
                signals = await self.poll_signals()
                for sig in signals:
                    stype = sig["type"]
                    payload = sig["payload"]

                    if stype == "offer" and not self.is_initiator:
                        sys_msg("Received SDP offer → sending answer…")
                        await self.pc.setRemoteDescription(
                            RTCSessionDescription(**payload)
                        )
                        # Flush buffered ICE candidates
                        for c in self.candidate_buffer:
                            await self.pc.addIceCandidate(c)
                        self.candidate_buffer.clear()
                        answer = await self.pc.createAnswer()
                        await self.pc.setLocalDescription(answer)
                        await self.send_signal("answer", {
                             "sdp": self.pc.localDescription.sdp,
                            "type": self.pc.localDescription.type,
                        })
                        sys_msg("Answer sent.")

                    elif stype == "answer" and self.is_initiator:
                        sys_msg("Received SDP answer.")
                        await self.pc.setRemoteDescription(
                            RTCSessionDescription(**payload)
                        )
                        for c in self.candidate_buffer:
                            await self.pc.addIceCandidate(c)
                        self.candidate_buffer.clear()

                    elif stype == "candidate":
                        if self.pc.remoteDescription is None:
                            # Can't add candidates before remote description — buffer them
                            self.candidate_buffer.append(payload)
                        else:
                            await self.pc.addIceCandidate(payload)

            except Exception as exc:
                if self.running:
                    sys_msg(f"Signal error: {exc}")

            if self.pc.connectionState in ("connected", "failed", "closed"):
                break
            await asyncio.sleep(0.3)

    # ── Main entry point ──────────────────────────────────────

    async def run(self):
        self.running = True

        # 1. Wait for peer
        await self._wait_for_peer()

        # 2. Create PeerConnection with public STUN server
        config = RTCConfiguration(
            iceServers=[
                RTCIceServer("stun:stun.l.google.com:19302"),
                RTCIceServer("stun:stun1.l.google.com:19302"),
            ]
        )
        self.pc = RTCPeerConnection(config)

        # Fired when the remote peer opens a DataChannel (responder side)
        @self.pc.on("datachannel")
        def on_datachannel(channel):
            self.channel = channel
            self._wire_channel(channel)

        @self.pc.on("connectionstatechange")
        def on_state():
            state = self.pc.connectionState
            if state == "connected":
                self._connected.set()
                sys_msg(
                    f"{BOLD}{GREEN}P2P connection established via WebRTC!{RESET}"
                )
                sys_msg(
                    "Chat is now fully peer-to-peer (DTLS encrypted). "
                    "The signaling server is no longer involved."
                )
                print()
            elif state == "failed":
                sys_msg(f"{RED}Connection failed — NAT traversal unsuccessful.{RESET}")
                sys_msg("Ensure both peers have internet access and try again.")
                self.running = False
            elif state == "closed":
                self.running = False

        @self.pc.on("icecandidate")
        def on_icecandidate(candidate):
            if candidate:
                asyncio.ensure_future(
                    self.send_signal("candidate", {
                        "candidate": candidate.candidate,
                        "sdpMid": candidate.sdpMid,
                        "sdpMLineIndex": candidate.sdpMLineIndex,
                    })
                )
        # 3. Initiator creates the DataChannel and sends an offer
        if self.is_initiator:
            self.channel = self.pc.createDataChannel("chat")
            self._wire_channel(self.channel)
            offer = await self.pc.createOffer()
            await self.pc.setLocalDescription(offer)
            sys_msg("Sent SDP offer.")
            await self.send_signal("offer", {
                "sdp": self.pc.localDescription.sdp,
                "type": self.pc.localDescription.type,
            })

        # 4. Run signal exchange in background
        sig_task = asyncio.create_task(self._signal_loop())

        # 5. Wait for P2P connection (with timeout)
        sys_msg("Exchanging SDP + ICE candidates…")
        try:
            await asyncio.wait_for(self._connected.wait(), timeout=30)
        except asyncio.TimeoutError:
            sys_msg(f"{RED}Connection timed out (30 s).{RESET}")
            sig_task.cancel()
            await self.pc.close()
            await self.leave_room()
            return

        # 6. Interactive chat loop
        loop = asyncio.get_event_loop()
        sys_msg("Type a message and press Enter. Commands: /help /status /quit")
        print()

        while self.running:
            try:
                line = await loop.run_in_executor(
                    None, lambda: input(f"{GREEN}> {RESET}")
                )
            except (EOFError, KeyboardInterrupt):
                break

            cmd = line.strip()
            if not cmd:
                continue

            if cmd == "/quit":
                break
            elif cmd == "/help":
                sys_msg("Commands: /quit — exit  |  /status — connection info  |  /help — this")
                continue
            elif cmd == "/status":
                if self.pc and self.channel:
                    sys_msg(
                        f"ICE state: {self.pc.iceConnectionState} | "
                        f"Channel: {self.channel.readyState} | "
                        f"Buffered: {self.channel.bufferedAmount} bytes"
                    )
                continue

            # Send chat message through the P2P DataChannel
            if self.channel and self.channel.readyState == "open":
                self.channel.send(cmd)
                # Show own message locally
                print(
                    f"\r{CYAN}{timestamp()} [you] {cmd}{RESET}\n"
                    f"{GREEN}> {RESET}",
                    end="",
                    flush=True,
                )
            else:
                sys_msg(f"{RED}Channel not open — peer may have disconnected.{RESET}")

        # 7. Clean up
        self.running = False
        sig_task.cancel()
        if self.pc:
            await self.pc.close()
        await self.leave_room()
        sys_msg("Disconnected. Goodbye.")


# ═══════════════════════════════════════════════════════════════
def main():
    if len(sys.argv) < 4:
        banner()
        print(f"Usage:  python messenger.py <SIGNAL_URL> <ROOM> <USERNAME>")
        print(f"Example:")
        print(f"  python messenger.py https://p2p-signal.user.workers.dev secret-room alice")
        print()
        sys.exit(1)

    signal_url = sys.argv[1]
    room = sys.argv[2]
    username = sys.argv[3]

    banner()
    print(f"{DIM}  Signal server : {signal_url}")
    print(f"  Room          : {room}")
    print(f"  User          : {username}")
    print(f"  Encryption    : DTLS (WebRTC mandatory){RESET}")
    print()

    messenger = Messenger(signal_url, room, username)
    try:
        asyncio.run(messenger.run())
    except KeyboardInterrupt:
        print(f"\n{DIM}Interrupted.{RESET}")


if __name__ == "__main__":
    main()
