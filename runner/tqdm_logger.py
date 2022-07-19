import logging
import tqdm


class TqdmLoggingHandler(logging.Handler):
    
    def __init__(self, level: str = logging.NOTSET):
        super().__init__(level)

    def emit(self, record: logging.LogRecord):
        try:
            msg = self.format(record)
            tqdm.tqdm.write(msg)
            self.flush()
        except Exception:
            self.handleError(record)


def setup_logging():
    # set up global tqdm logger
    logging.basicConfig(format='%(asctime)s %(message)s', datefmt='[%d %b %H:%M]', handlers=[TqdmLoggingHandler()])
