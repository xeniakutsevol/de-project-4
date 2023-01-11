class IncrementCounter:

    def __init__(self):
        self._value = 0

    def new_value(self):
        self._value += 50
        return self._value