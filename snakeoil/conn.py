import simple_salesforce

__author__ = 'jawaad'


def conn(username, password, security_token):
    return simple_salesforce.Salesforce(username=username,
                                        password=password,
                                        security_token=security_token)


def generate_sandbox_connections(rng, username, password, security_token):
    for x in xrange(0, rng):
        yield simple_salesforce.Salesforce(username, password, security_token)
