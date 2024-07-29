package com.sproutsocial.nsqauthj.resources;

import com.sproutsocial.platform.dropwizard.commons.healthcheck.HealthCheckResource;

import javax.ws.rs.Path;

/*
 * Super simple healthcheck endpoint.  This is needed to match the spec for the older
 * Python version of the application.
 */
@Path("/ping")
public class PingResource extends HealthCheckResource {
    public PingResource(String serviceName) {
        super(serviceName);
    }
}
