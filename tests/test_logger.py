import logging
import unittest

from windnf.logger import setup_logger


class TestLoggerLevels(unittest.TestCase):
    def tearDown(self) -> None:
        for name in ("windnf-test-levels", "windnf-test-reconfigure"):
            logger = logging.getLogger(name)
            logger.handlers.clear()
            logger.propagate = True

    def test_setup_logger_parses_aliases_and_fallback(self) -> None:
        logger = setup_logger(name="windnf-test-levels", level="warn")
        self.assertEqual(logger.level, logging.WARNING)
        self.assertEqual(logger.handlers[0].level, logging.WARNING)

        logger = setup_logger(name="windnf-test-levels", level="fatal")
        self.assertEqual(logger.level, logging.CRITICAL)
        self.assertEqual(logger.handlers[0].level, logging.CRITICAL)

        logger = setup_logger(name="windnf-test-levels", level="nope")
        self.assertEqual(logger.level, logging.INFO)
        self.assertEqual(logger.handlers[0].level, logging.INFO)

    def test_setup_logger_reconfigures_existing_handlers(self) -> None:
        name = "windnf-test-reconfigure"

        logger = setup_logger(name=name, level="info")
        self.assertEqual(logger.level, logging.INFO)
        self.assertEqual(len(logger.handlers), 1)
        self.assertEqual(logger.handlers[0].level, logging.INFO)

        logger = setup_logger(name=name, level="debug")
        self.assertEqual(logger.level, logging.DEBUG)
        self.assertEqual(len(logger.handlers), 1)
        self.assertEqual(logger.handlers[0].level, logging.DEBUG)


if __name__ == "__main__":
    unittest.main()
