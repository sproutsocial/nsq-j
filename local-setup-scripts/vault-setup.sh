#!/usr/bin/env sh
set -ex
export VAULT_ADDR=http://vault:8200




vault kv put secret/services/nsq/user-tokens/1234 username="example_user_token" topics=".*"
vault kv put secret/services/nsq/service-tokens/abcd username="example_service_token" topics=".*"
