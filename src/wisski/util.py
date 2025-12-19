from functools import wraps
import inspect


def call(func, *args, **kwargs):
    """Call a function with only the arguments it needs"""
    sig = inspect.signature(func)
    bound_args = {}

    # Map positional arguments
    for name, param in list(sig.parameters.items()):
        if args:
            bound_args[name] = args[0]
            args = args[1:]
        elif name in kwargs:
            bound_args[name] = kwargs[name]

    return func(**bound_args)


def pre(pre_func):
    """Run pre_func before the decorated function."""

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            pre_func(*args)
            return call(func, *args, **kwargs)

        return wrapper

    return decorator


def post(post_func):
    """Run post_func after the decorated function."""

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            result = func(*args, **kwargs)
            # Add the result to the kwargs
            kwargs["result"] = result
            call(post_func, *args, **kwargs)
            return result

        return wrapper

    return decorator


def log(logger, level, template):
    """
    Lazy logging decorator.

    Parameters:
        logger : logging.Logger instance
        level : logging level (logging.INFO, logging.DEBUG, etc.)
        template : string template using {arg_name} placeholders
    """

    def decorator(func):
        sig = inspect.signature(func)

        @wraps(func)
        def wrapper(*args, **kwargs):
            # Bind the arguments to parameter names
            bound = sig.bind(*args, **kwargs)
            bound.apply_defaults()

            # Evaluate template lazily
            try:
                message = template.format(**bound.arguments)
            except AttributeError:
                message = template  # fallback if something is missing

            logger.log(level, message)

            return func(*args, **kwargs)

        return wrapper

    return decorator
