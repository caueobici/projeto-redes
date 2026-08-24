"""Servidor do Monitor do Sistema (Projeto Prático 1 - Fase 1).

Atende um único cliente. Fluxo: socket -> bind -> listen -> accept -> MSG1,
depois duas threads por conexão:
  - Thread 1: lê comandos do socket e cria/encerra threads de monitor.
  - Thread 2: envia ao cliente as saídas que os monitores colocam na fila.

Uso: python3 server.py [host] [porta]
"""

import queue
import re
import socket
import subprocess
import sys
import threading
from datetime import datetime

HOST_PADRAO = "0.0.0.0"
PORTA_PADRAO = 5000

MENU = """\
=================== MONITOR DO SISTEMA ===================
Monitores disponiveis (N = periodo em segundos, padrao 1):
  CPU-N        uso de CPU do servidor a cada N segundos
  MEMORIA-N    uso de memoria do servidor a cada N segundos
Outros comandos:
  LIST         lista os monitores ativos
  QUIT-ID      encerra o monitor de numero ID
  QUIT         encerra todos os monitores
  MENU         mostra este menu
  EXIT         encerra todas as threads e sai
Exemplo: CPU-5
=========================================================="""


def agora():
    return datetime.now().strftime("%H:%M:%S")


# ---------------------------------------------------------------------------
# Leitura das métricas (Linux via /proc, macOS via top/sysctl/vm_stat)
# ---------------------------------------------------------------------------

def _ler_proc_stat():
    """Retorna (ocioso, total) acumulados de /proc/stat (Linux)."""
    with open("/proc/stat") as f:
        campos = [int(x) for x in f.readline().split()[1:]]
    ocioso = campos[3] + campos[4]  # idle + iowait
    return ocioso, sum(campos)


def _parse_top_mac(texto):
    """Extrai o uso de CPU (%) da última linha 'CPU usage' do top do macOS."""
    linhas = re.findall(r"CPU usage:.*?([\d.]+)% idle", texto)
    if not linhas:
        raise ValueError("saida do top sem 'CPU usage'")
    return 100.0 - float(linhas[-1])


def _parse_vm_stat(texto):
    """Retorna os bytes disponíveis (free + inactive + speculative) do vm_stat."""
    m = re.search(r"page size of (\d+) bytes", texto)
    if not m:
        raise ValueError("saida do vm_stat sem tamanho de pagina")
    tam_pagina = int(m.group(1))
    paginas = 0
    for nome in ("free", "inactive", "speculative"):
        m = re.search(r"Pages %s:\s+(\d+)" % nome, texto)
        if m:
            paginas += int(m.group(1))
    return paginas * tam_pagina


def _executar(*cmd):
    return subprocess.run(cmd, capture_output=True, text=True, check=True).stdout


class MedidorCPU:
    """Mede o uso de CPU desde a medição anterior (Linux) ou no último segundo (macOS)."""

    def __init__(self):
        self.anterior = _ler_proc_stat() if sys.platform.startswith("linux") else None

    def medir(self):
        if sys.platform.startswith("linux"):
            atual = _ler_proc_stat()
            d_ocioso = atual[0] - self.anterior[0]
            d_total = atual[1] - self.anterior[1]
            self.anterior = atual
            return 0.0 if d_total == 0 else 100.0 * (1 - d_ocioso / d_total)
        if sys.platform == "darwin":
            # A primeira amostra do top no macOS não é confiável; usa a segunda.
            return _parse_top_mac(_executar("top", "-l", "2", "-n", "0", "-s", "1"))
        raise NotImplementedError


def ler_memoria():
    """Retorna (usado, total) em bytes."""
    if sys.platform.startswith("linux"):
        info = {}
        with open("/proc/meminfo") as f:
            for linha in f:
                chave, valor = linha.split(":", 1)
                info[chave] = int(valor.split()[0]) * 1024
        total = info["MemTotal"]
        return total - info["MemAvailable"], total
    if sys.platform == "darwin":
        total = int(_executar("sysctl", "-n", "hw.memsize"))
        return total - _parse_vm_stat(_executar("vm_stat")), total
    raise NotImplementedError


# ---------------------------------------------------------------------------
# Sessão: memória compartilhada entre as threads da conexão
# ---------------------------------------------------------------------------

class Session:
    def __init__(self, conn):
        self.conn = conn
        self.saida = queue.Queue()
        self.encerrar = threading.Event()
        self.monitores = {}
        self.lock = threading.Lock()
        self.proximo_id = 1

    def enviar(self, texto):
        self.saida.put(texto)

    # --- threads do monitor ---------------------------------------------

    def iniciar_monitor(self, tipo, periodo):
        parar = threading.Event()
        with self.lock:
            mid = self.proximo_id
            self.proximo_id += 1
            t = threading.Thread(target=self._monitor, args=(mid, tipo, periodo, parar),
                                 name="monitor-%d" % mid)
            self.monitores[mid] = (tipo, periodo, parar, t)
        t.start()
        self.enviar("Monitor #%d (%s a cada %ss) iniciado." % (mid, tipo, periodo))

    def _monitor(self, mid, tipo, periodo, parar):
        try:
            medidor = MedidorCPU() if tipo == "CPU" else None
        except Exception as e:
            self.enviar("#%d %s: erro ao ler metrica (%s)" % (mid, tipo, e))
            return
        while not parar.wait(periodo):
            try:
                if tipo == "CPU":
                    valor = "%.1f%%" % medidor.medir()
                else:
                    usado, total = ler_memoria()
                    gib = 1024 ** 3
                    valor = "%.2f/%.2f GiB (%.1f%%)" % (usado / gib, total / gib,
                                                        100.0 * usado / total)
            except NotImplementedError:
                valor = "metrica nao suportada neste sistema (%s)" % sys.platform
            except Exception as e:
                valor = "erro ao ler metrica (%s)" % e
            if not parar.is_set():
                self.enviar("[%s] #%d %s: %s" % (agora(), mid, tipo, valor))

    def parar_monitor(self, mid):
        with self.lock:
            item = self.monitores.pop(mid, None)
        if item is None:
            self.enviar("Monitor #%d nao existe." % mid)
            return
        item[2].set()
        self.enviar("Monitor #%d (%s) encerrado." % (mid, item[0]))

    def parar_todos(self):
        with self.lock:
            itens = list(self.monitores.items())
            self.monitores.clear()
        for _, (_, _, parar, _) in itens:
            parar.set()
        for _, (_, _, _, t) in itens:
            t.join()
        return len(itens)

    def listar(self):
        with self.lock:
            itens = sorted(self.monitores.items())
        if not itens:
            return "Nenhum monitor ativo."
        return "Monitores ativos:\n" + "\n".join(
            "  #%d %s a cada %ss" % (mid, tipo, periodo) for mid, (tipo, periodo, _, _) in itens)

    # --- thread 1: leitura do socket --------------------------------------

    def processar(self, linha):
        cmd = linha.strip().upper().replace("Ó", "O")
        if not cmd:
            return
        print("[%s] comando recebido: %s" % (agora(), linha.strip()))

        m = re.fullmatch(r"(CPU|MEM|MEMORIA)(?:-(\d+))?", cmd)
        if m:
            tipo = "CPU" if m.group(1) == "CPU" else "MEMORIA"
            periodo = int(m.group(2) or 1)
            if periodo < 1:
                self.enviar("Periodo deve ser >= 1 segundo.")
            else:
                self.iniciar_monitor(tipo, periodo)
            return

        m = re.fullmatch(r"QUIT(?:-(\d+))?", cmd)
        if m:
            if m.group(1):
                self.parar_monitor(int(m.group(1)))
            else:
                self.enviar("%d monitor(es) encerrado(s)." % self.parar_todos())
            return

        if cmd == "LIST":
            self.enviar(self.listar())
        elif cmd == "MENU":
            self.enviar(MENU)
        elif cmd == "EXIT":
            self.parar_todos()
            self.enviar("Encerrando conexao. Ate logo!")
            self.encerrar.set()
        else:
            self.enviar("Comando invalido: '%s'. Digite MENU para ver os comandos." % linha.strip())

    def thread_leitura(self):
        buffer = ""
        while not self.encerrar.is_set():
            dados = self.conn.recv(1024)
            if not dados:  # cliente fechou a conexão
                print("[%s] cliente desconectou." % agora())
                self.parar_todos()
                self.encerrar.set()
                break
            buffer += dados.decode("utf-8")
            while "\n" in buffer and not self.encerrar.is_set():
                linha, buffer = buffer.split("\n", 1)
                self.processar(linha)

    # --- thread 2: envio ao cliente ---------------------------------------

    def thread_envio(self):
        while not (self.encerrar.is_set() and self.saida.empty()):
            try:
                texto = self.saida.get(timeout=0.5)
            except queue.Empty:
                continue
            self.conn.sendall((texto + "\n").encode("utf-8"))
        # Fecha a conexão: o cliente recebe EOF e encerra sua thread 2.
        try:
            self.conn.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass  # o cliente ja tinha fechado


def main():
    host = sys.argv[1] if len(sys.argv) > 1 else HOST_PADRAO
    porta = int(sys.argv[2]) if len(sys.argv) > 2 else PORTA_PADRAO

    servidor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    servidor.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    servidor.bind((host, porta))
    servidor.listen(1)
    print("[%s] servidor aguardando conexao em %s:%d ..." % (agora(), host, porta))

    conn, address = servidor.accept()
    servidor.close()  # apenas um usuário remoto
    print("[%s] cliente conectado: %s:%d" % (agora(), *address))

    sessao = Session(conn)
    sessao.enviar("%s: CONECTADO!!" % agora())  # CONECTADO!
    sessao.enviar(MENU)

    t1 = threading.Thread(target=sessao.thread_leitura, name="thread-1-leitura")
    t2 = threading.Thread(target=sessao.thread_envio, name="thread-2-envio")
    t1.start()
    t2.start()

    # encerra se todas as threads terminarem
    t1.join()
    t2.join()
    conn.close()
    print("[%s] servidor encerrado." % agora())


if __name__ == "__main__":
    main()
