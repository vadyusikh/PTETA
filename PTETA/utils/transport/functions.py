def cast_if_possible(value, cast_function, default=None):
    if (value is None) or (value == ''):
        return default
    try:
        return cast_function(value)
    except:
        return default
