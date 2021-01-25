package com.sproutsocial.nsqauthj.resources;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.annotation.Timed;
import com.google.inject.Inject;
import com.sproutsocial.nsqauthj.permissions.NsqPermissionSet;
import com.sproutsocial.nsqauthj.tokens.NsqToken;
import com.sproutsocial.nsqauthj.validators.VaultTokenValidator;
import com.sproutsocial.platform.Heartbeater;
import org.hibernate.validator.constraints.NotEmpty;

import javax.servlet.http.HttpServletRequest;
import javax.validation.constraints.NotNull;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Optional;

@Path("/auth")
public class AuthResource {
    private final VaultTokenValidator vaultTokenValidator;
    private final Heartbeater heartbeater;
    private final MetricRegistry metrics;


    @Inject
    public AuthResource(
            VaultTokenValidator validator,
            Heartbeater heartbeater,
            MetricRegistry metrics) {
        this.vaultTokenValidator = validator;
        this.heartbeater = heartbeater;
        this.metrics = metrics;
    }

    @GET
    @Path("")
    @Timed
    public Response validateWithVault(
            @QueryParam("secret") @NotEmpty @NotNull String tokenString,
            @Context HttpServletRequest request) {
        Optional<NsqToken> nsqToken = vaultTokenValidator.validateToken(tokenString, request.getRemoteAddr());
        NsqPermissionSet nsqPermissionSet = NsqPermissionSet.fromNsqToken(nsqToken.get(),
          vaultTokenValidator.getFailOpen());
        heartbeater.sendHeartBeat("nsqauthj", "nsqauthj");
        return Response.ok().type(MediaType.APPLICATION_JSON).entity(nsqPermissionSet).build();
    }
}
