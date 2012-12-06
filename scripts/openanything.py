import urllib2

class DefaultErrorHandler(urllib2.HTTPDefaultErrorHandler):    
    def http_error_default(self, req, fp, code, msg, headers): 
        result = urllib2.HTTPError(                           
            req.get_full_url(), code, msg, headers, fp)       
        result.status = code                                   
        return result