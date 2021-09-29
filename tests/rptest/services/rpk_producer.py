from ducktape.services.background_thread import BackgroundThreadService


class RpkProducer(BackgroundThreadService):
    """
    Wrap the `rpk topic produce` command.  This is useful if you need
    to write a simple repeated value to a topic, for example to increase
    the topic's size on disk when testing recovery.
    """
    def __init__(self, context, redpanda, topic, msg_size, msg_count):
        super(RpkProducer, self).__init__(context, num_nodes=1)
        self._redpanda = redpanda
        self._topic = topic
        self._msg_size = msg_size
        self._msg_count = msg_count

    def _worker(self, _idx, node):
        rpk_binary = self._redpanda.find_binary("rpk")
        cmd = f"dd if=/dev/zero bs={self._msg_size} count=1 | {rpk_binary} topic --brokers {self._redpanda.brokers()} produce -n {self._msg_count} --key test {self._topic}"
        for line in node.account.ssh_capture(cmd, timeout_sec=10):
            self.logger.debug(line.rstrip())

    def stop_node(self, node):
        node.account.kill_process("rpk", clean_shutdown=False)
