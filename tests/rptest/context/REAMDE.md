The Context package contains "accessors" for Ducktape's [TestContext][1]
tailored for our domain and that can be shared by different services or that
can used directly by specific tests. Accessors are expected to be stateless.

If state is needed, consider wrapping it in a Ducktape [Service][2]
(num_nodes=0) which can provide centralized state management and lifecycle
management. Not only it is a good practice of encapsulating state, but Ducktape
machinery will ensure appropriate stop and cleanup methods are called in the
correct order which allow you then to e.g. cleanup remote state (object storage
buckets, databases, etc.) in a reliable way.

There isn't a strict rule for the granularity of the services. You'll have to
use your judgement to decide if something is a service with methods to manage
resources (i.e. bucket) or whether a service per resource is needed (i.e. each
bucket is a service). When in doubt, start with a single service and refactor
later if needed.

Note: Ducktape will call cleanup methods in the order of service registration.
Meaning that if you register say a Bucket service and then a Database service
that depends on the bucket, then bucket service will have its cleanup method
called first. If the database must be cleaned up first, then you can do the
clean up in the stop method. **We should fix this and cleanup in reverse
registration order too.**

[1]: https://github.com/confluentinc/ducktape/blob/6136efbb84710f7f7a200172ae88873ab154582d/ducktape/tests/test.py#L262
[2]: https://github.com/confluentinc/ducktape/blob/6136efbb84710f7f7a200172ae88873ab154582d/ducktape/services/service.py#L51
