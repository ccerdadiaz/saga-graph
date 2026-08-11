package sagagraph

enum SagaResult:
  case Completed
  case Failed(cause: Throwable)