"""Cliente do Monitor do Sistema (Projeto Prático 1 - Fase 2).

Fluxo: socket -> connect, depois duas threads:
  - Thread 1: lê comandos do teclado e envia ao servidor.
  - Thread 2: recebe dados do servidor e imprime na tela.

Uso: python3 client.py [host] [porta]
"""

import socket
import sys
import threading

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 5000


def thread_teclado(sock, desconectado, saindo):
    """Thread 1: lê do teclado e envia pelo socket (loop infinito até EXIT)."""
    while True:
        try:
            linha = input()
        except EOFError:  # Ctrl+D: trata como EXIT
            linha = "EXIT"
        if desconectado.is_set():
            break
        sair = linha.strip().upper() == "EXIT"
        if sair:
            saindo.set()
        sock.sendall((linha + "\n").encode("utf-8"))
        if sair:
            break


def thread_tela(sock, desconectado, saindo):
    """Thread 2: lê do socket e imprime na tela (loop infinito até o servidor fechar)."""
    while True:
        dados = sock.recv(4096)
        if not dados:
            break
        print(dados.decode("utf-8"), end="", flush=True)
    desconectado.set()
    if not saindo.is_set():
        print("Conexao encerrada pelo servidor.")


def main():
    host = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_HOST
    porta = int(sys.argv[2]) if len(sys.argv) > 2 else DEFAULT_PORT

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((host, porta))

    desconectado = threading.Event()  # servidor fechou a conexão
    saindo = threading.Event()        # usuário digitou EXIT
    t1 = threading.Thread(target=thread_teclado, args=(sock, desconectado, saindo),
                          name="thread-1-teclado", daemon=True)
    t2 = threading.Thread(target=thread_tela, args=(sock, desconectado, saindo), name="thread-2-tela")
    t1.start()
    t2.start()

    t2.join()
    if saindo.is_set():
        t1.join()
    sock.close()


if __name__ == "__main__":
    main()
