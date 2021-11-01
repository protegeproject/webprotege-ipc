package edu.stanford.protege.webprotege.ipc;

import edu.stanford.protege.webprotege.authorization.ActionId;
import edu.stanford.protege.webprotege.authorization.Resource;
import edu.stanford.protege.webprotege.common.Request;
import edu.stanford.protege.webprotege.common.Response;

import javax.annotation.Nonnull;
import java.util.Collection;

/**
 * Matthew Horridge
 * Stanford Center for Biomedical Informatics Research
 * 2021-10-11
 *
 * A {@link CommandHandler} for commands that require authorization.  The handler determines the particular resource
 * for a given request and specifies the required permission/capability that is required to access the resource.
 */
public interface AuthorizedCommandHandler<Q extends Request<R>, R extends Response> extends CommandHandler<Q, R> {

    @Nonnull
    Resource getTargetResource(Q request);

    @Nonnull
    Collection<ActionId> getRequiredCapabilities();
}
