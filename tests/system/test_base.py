from kafkabeat import BaseTest

import os


class Test(BaseTest):

    def test_base(self):
        """
        Basic test with exiting Kafkabeat normally
        """
        self.render_config_template(
                path=os.path.abspath(self.working_dir) + "/log/*"
        )

        kafkabeat_proc = self.start_beat()
        self.wait_until( lambda: self.log_contains("kafkabeat is running"))
        exit_code = kafkabeat_proc.kill_and_wait()
        assert exit_code == 0
