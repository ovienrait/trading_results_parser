import logging

from tqdm import tqdm


class TqdmLoggingHandler(logging.Handler):
    """Кастомный обработчик логов для tqdm для вывода сообщений в консоль."""

    def emit(self, record):
        try:
            msg = self.format(record)
            tqdm.write(msg)
            self.flush()
        except Exception:
            self.handleError(record)


def setup_logger(
    log_file: str = 'logs/parser.log', logger_name: str = 'parser'
) -> logging.Logger:
    """Настраивает логгер для приложения."""

    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)
    logger.propagate = False

    if logger.hasHandlers():
        logger.handlers.clear()

    console_handler = TqdmLoggingHandler()
    console_handler.setFormatter(logging.Formatter(
        '[%(asctime)s] %(levelname)s: %(message)s', '%H:%M:%S'
    ))

    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setFormatter(
        logging.Formatter(
            '[%(asctime)s] %(levelname)s: %(message)s', '%Y-%m-%d %H:%M:%S'
        )
    )

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger
