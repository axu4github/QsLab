# -*- coding: utf-8 -*-

"""
测试 SparkSQL 并行查询处理能力
"""

import click
import sys
reload(sys)
sys.setdefaultencoding("utf-8")

# click 模块配置
CLICK_CONTEXT_SETTINGS = dict(
    help_option_names=["-h", "--help"], terminal_width=100)


@click.command(context_settings=CLICK_CONTEXT_SETTINGS)
@click.option("--parallels", default=1, help="并行数量")
def main(parallels):
    print(parallels)


if __name__ == "__main__":
    main()
