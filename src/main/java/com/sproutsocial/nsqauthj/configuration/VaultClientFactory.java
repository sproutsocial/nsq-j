package com.sproutsocial.nsqauthj.configuration;

import com.bettercloud.vault.Vault;
import com.bettercloud.vault.VaultException;
import com.bettercloud.vault.response.AuthResponse;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        property = "type"
)
@JsonSubTypes({
        @JsonSubTypes.Type(name = "token", value = TokenClientFactory.class),
        @JsonSubTypes.Type(name = "approle", value = ApproleClientFactory.class),
        @JsonSubTypes.Type(name = "jwt", value = JwtClientFactory.class)
})
public interface VaultClientFactory {
    Vault build() throws Exception;


}
