### secret-p2p-messager

This is a p2p messager, if you have any issue open an issue or pull request, Contributors are welcomed

## requirements
python
npm >= 22.0.0
pip

## SETUP

# clone the repo and cd to the project folder

```text
git clone https://github.com/ZevricKernel/secret-p2p-messager.git

cd secret-p2p-messager/server
```

# setup the server

install wrangler using npm:
```text
npm install -g wrangler
```
then log in using your cloudflare account 
```text
wrangler login

```
create a KV namespace:
```text
wrangler kv namespace create SIGNAL_KV
```
copy the ID of the KV namespace and put it inside /server/wrangler.toml in id = "PASTE YOUR KV ID HERE"

then run

```text

npx wrangler deploy

```

To deploy the project to cloudflare workers

It gives you a URL like https://p2p-signal.yourname.workers.dev
copy the url. the run this to move to client folder:
```text
cd ../client
```
OR
just open a terminal inside the client folder
then install the needed python libraries:
```text
pip install -r requirements.txt
```
then run the messenger:
```text
python messenger.py your-URL secret-room yourname
```
connect another pear to the same worker and room
# FINISH
your now can use your private p2p messenger
















