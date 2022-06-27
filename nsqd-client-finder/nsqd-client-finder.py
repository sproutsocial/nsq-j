import argparse
from collections import defaultdict
import json
import urllib.parse

import requests


class NSQClientFinder:
    def __init__(self, nsq_lookup_address="http://prod-nsq-lookup-useast1b-201:4161"):
        self.nsq_lookupd_address=nsq_lookup_address

    def find_nsqd_hosts(self):
        nodes_path='/nodes'
        full_url = urllib.parse.urljoin(self.nsq_lookupd_address, nodes_path)
        response = requests.get(full_url, timeout=3)
        found_nsqd_hosts = [
            "http://{}:{}".format(x['hostname'], x['http_port'])
            for x in response.json()['data']['producers']
        ]
        if not response.ok:
            raise Exception("Bad response from nsqlookupd with status code: %s" % response.status_code)
        return found_nsqd_hosts

    def normalize_k8s_name(self, hostname):
        """
        "Normalize" a k8s pod name if the hostname is a pod.  If not, just return
        """
        # Cheat and assume if it has '-useast1' in the hostname, it's an ec2 instance
        if '-useast1' in hostname:
            return hostname

        return 'k8s-pod-%s' % '-'.join(hostname.split('-')[:-2])

    def get_clients_for_nsqd(self, nsqd_host):
        stats_path='/stats'
        get_params={
            'format': 'json',
            'include_clients': True
        }
        full_url = urllib.parse.urljoin(nsqd_host, stats_path)
        identity_auth_host_map = defaultdict(set)
        try:
            response = requests.get(full_url, timeout=3, params=get_params)
            if not response.ok:
                print("Bad response from nsqd %s with status code %s" % (nsqd_host, response.status_code))
                return
            # Extract all client information
            # Clients are specific to channels and channels to topics
            # Collapse these into a map of username -> set of all hosts (or pods) using that username
            for topic in response.json()['data']['topics']:
                for channels in topic['channels']:
                    for client in channels['clients']:
                        # We care about hostname and auth_identity
                        # hostname is the pod name in k8s land so normalize it
                        hostname = self.normalize_k8s_name(client['hostname'])
                        auth_identity = client['auth_identity']
                        identity_auth_host_map[auth_identity].add(hostname)

        except Exception as e:
            print(e)
        return identity_auth_host_map

    def run(self):
        found_nsqd_hosts = self.find_nsqd_hosts()
        all_identity_auth_host_map = defaultdict(set)
        for nsqd_host in found_nsqd_hosts:
            identity_auth_host_map = self.get_clients_for_nsqd(nsqd_host)

            for key, value in identity_auth_host_map.items():
                existing = all_identity_auth_host_map[key]
                existing.update(value)
                all_identity_auth_host_map[key] = existing
        return all_identity_auth_host_map


class SetEncoder(json.JSONEncoder):
    def default(self, obj):
        return list(obj)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Display nsqd connected client information.')
    parser.add_argument(
        '--nsq-lookupd-address',
        dest='nsq_lookupd_address',
        default='http://prod-nsq-lookup-useast1b-201:4161',
        help='nsq-lookupd address to query (defaults to http://prod-nsq-lookup-useast1b-201:4161)')

    args = parser.parse_args()

    finder = NSQClientFinder(args.nsq_lookupd_address)
    results = finder.run()
    print(json.dumps(results, indent=4, cls=SetEncoder))


