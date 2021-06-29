import functools


def zielezin_wrapper(func):
  @functools.wraps(func)  # behält func.__name__ und func.__doc__ ursprunglicher Funktion
  def wrapper(self, *args, **kwargs):
    # print Funktionsaufruf
    args_list = [repr(arg) for arg in args]
    kwargs_list = [f"{k} = {v!r}" for k, v in kwargs.items()]
    all_args = ', '.join(args_list + kwargs_list)
    # if self.level > 0: print()
    print(f"\n{self.spaces}{func.__name__}({all_args}):")

    self.level += 1
    startzeit = self._start()
    self.level += 1
    value = func(self, *args, **kwargs)
    self.level -= 1
    self._ende(startzeit)
    self.level -= 1
    # if self.level > 0: print()
    return value
  # /def wrapper(self, *args, **kwargs)
  return wrapper
# /def zielezin_wrapper(func)
