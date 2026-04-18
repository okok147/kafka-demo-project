class AppError(Exception):
    pass


class DomainValidationError(AppError):
    pass


class DuplicateRequestError(AppError):
    pass
