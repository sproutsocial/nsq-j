package com.sproutsocial.nsqauthj.resources;

import com.google.inject.Inject;
import com.sproutsocial.nsqauthj.permissions.NsqPermissionSet;
import com.sproutsocial.nsqauthj.tokens.NsqToken;
import com.sproutsocial.nsqauthj.validators.VaultTokenValidator;
import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.constraints.NotNull;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Optional;

@Path("/auth")
public class AuthResource {
    private final VaultTokenValidator vaultTokenValidator;


    @Inject
    public AuthResource(
            VaultTokenValidator validator
    ) {
        this.vaultTokenValidator = validator;
    }

    @GET
    @Path("")
    @Produces({MediaType.TEXT_HTML, MediaType.APPLICATION_JSON})
    public Response validateWithVault(@QueryParam("token") @NotEmpty @NotNull String tokenString) {
        Optional<NsqToken> nsqToken = vaultTokenValidator.validateToken(tokenString);
        return Response.ok().entity(NsqPermissionSet.fromNsqToken(nsqToken.get())).build();
    }
}
