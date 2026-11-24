from scripts.process_manager import Manager


def stop():
    manager = Manager()
    for record in reversed(manager.list()):
        print(manager.stop(record['name']))


if __name__ == '__main__':
    stop()

