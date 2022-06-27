nsqd-client-finder
------------------

`nsqd-client-finder` is a small utility for finding all clients connected in a nsq ecosystem and mapping the authenticated usernames to the host (or k8s pod) they originate from.  This provides a mapping of:

```
{
    "platform_service_token_from_vault_2022": [
        "prod-data-ingest-useast1a-02",
        "k8s-pod-sage-engine",
        <etc>
    ],
    "platform_service_token_from_vault_2021": [
        "k8s-pod-instagram-attribution",
        "k8s-pod-fb-pm-attachment-storage",
        "prod-inbox-bus-useast1a-02",
        <etc>
    ]
}
```

### Setup

Create a virtual environment with the tool of your choosing and install requirements like
```
pip install -r requirements.txt
```

### Running the application

```

❯ python nsqd-client-finder.py --help
usage: nsqd-client-finder.py [-h] [--nsq-lookupd-address NSQ_LOOKUPD_ADDRESS]

Display nsqd connected client information.

optional arguments:
  -h, --help            show this help message and exit
    --nsq-lookupd-address NSQ_LOOKUPD_ADDRESS
                            nsq-lookupd address to query (defaults to http://prod-nsq-lookup-useast1b-201:4161)
```

To run the application, do the following
```
python nsqd-client-finder.py
```
