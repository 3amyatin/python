from datetime import datetime, timedelta

from config import _config as config
from . import ExasolConnection


class Hid(str):
  pass


class General:
  level: int = 0
  gesamtdauer: timedelta = timedelta()

  def get_spaces(self, adjustment:int = 0):
    return " " * 2 * (self.level + adjustment)
  spaces: str = property(get_spaces)

  def __init__(self,driver='sqlalchemy', env='prod', echo=False):
    self.driver = driver
    self.env = env
    self.echo = echo
    self._hid = Hid()

    if driver == 'sqlalchemy':
      self.exasol_prod = ExasolConnection(**config.connect['prod-sqlalchemy'], _echo=self.echo)
      # self.exasol_dev = ExasolConnection(**config.connect['dev-sqlalchemy'], _echo=self.echo)

    elif driver == 'pyodbc':
      self.exasol_prod = ExasolConnection(**config.connect['prod-pyodbc'])
      # self.exasol_dev = ExasolConnection(**config.connect['dev-pyodbc'])

    elif driver == 'pyexasol':
      self.exasol_prod = ExasolConnection(**config.connect['prod-pyexasol'])
      # self.exasol_dev = ExasolConnection(**config.connect['dev-pyexasol'])

    else:  # /if driver unbekannt
      raise Exception(f"Unbekannter Exasol-Treiber {driver}")

    self.exasol_set_env(env)
  # /def __init__()

  def get_hid(self) -> str:
    self._hid = Hid(datetime.now().strftime("%d%m%Y%H%M%S"))
    return self._hid

  def multiline_spaces(self, s: str, *, spaces = None, lstrip=False):
    if not spaces:
      spaces = self.spaces
    s_list = s.splitlines()
    if lstrip:
      s_list = [line.lstrip() for line in s_list]
    s_list = [spaces + line for line in s_list]

    return '\n'.join(s_list)

  def exasol_set_env(self, env: str):
    if env == 'dev':    self.exasol = self.exasol_dev
    elif env == 'prod': self.exasol = self.exasol_prod
    else:               raise Exception(f'Unbekannte Exasol-Umgebung {env!r}')
    self.SQLError = self.exasol.SQLError
    return self.exasol

  def _start(self):
    self.startzeit = datetime.now()
    print(self.spaces + "Beginn: " + self.startzeit.strftime("%d.%m.%Y %H:%M:%S"))
    return self.startzeit

  def _ende(self, startzeit: datetime = None):
    if startzeit:
      self.startzeit = startzeit
    self.endzeit = datetime.now()
    aufgaben_dauer = self.endzeit - self.startzeit
    self.gesamtdauer = self.gesamtdauer + aufgaben_dauer
    print(self.spaces + "Ende: " + self.endzeit.strftime('%d.%m.%Y %H:%M:%S'))
    print(self.spaces + "Dauer: " + str(aufgaben_dauer))

  def beenden(self):
    print("\nGesamtdauer: " + str(self.gesamtdauer))

  def __del__(self):
    self.beenden()

# /class General:
