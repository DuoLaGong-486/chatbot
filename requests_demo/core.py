import contextvars


HTTP_CTX = contextvars.ContextVar("HTTP_CTX",default=None)
