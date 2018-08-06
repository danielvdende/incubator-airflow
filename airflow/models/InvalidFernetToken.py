class InvalidFernetToken(Exception):
    # If Fernet isn't loaded we need a valid exception class to catch. If it is
    # loaded this will get reset to the actual class once get_fernet() is called
    pass
