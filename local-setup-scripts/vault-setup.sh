#!/usr/bin/env sh
set -ex
export VAULT_ADDR=http://testing-vault:8200

vault auth enable approle

vault policy write nsqd-auth-validation ./nsqd-auth-validation.hcl

vault write auth/approle/role/nsqauthj policies="nsqd-auth-validation" token_ttl="60" period="24h"

vault read auth/approle/role/nsqauthj/role-id
vault write -f auth/approle/role/nsqauthj/secret-id

vault kv put secret/services/nsq/user-tokens/1234 username="example_user_token" topics=".*"
vault kv put secret/services/nsq/service-tokens/abcd username="example_service_token" topics=".*"
