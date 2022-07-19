class Device:
    index = 0

    def __init__(self, *_, **__):
        pass

    @staticmethod
    def count() -> int:
        return 1

    @staticmethod
    def gpu_utilization() -> float:
        return 0.0

    @staticmethod
    def memory_total() -> float:
        return 9999999.9

    @staticmethod
    def processes() -> dict:
        return {}
