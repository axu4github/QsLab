# -*- coding: utf-8 -*-

""" 多关键词精准匹配 """

from functools import wraps
from time import clock
import ahocorasick
import jieba


def time_analyze(func):
    """ 装饰器 获取程序执行时间 """
    @wraps(func)
    def consume(*args, **kwargs):
        # 重复执行次数（单次执行速度太快）
        exec_times = 1
        start = clock()
        for i in range(exec_times):
            r = func(*args, **kwargs)

        finish = clock()
        print "{:<20}{:10.6} s".format(func.__name__ + ":", finish - start)
        return r

    return consume


@time_analyze
def common_algorithm(kws, context):
    """ 使用 Python 默认 字符串查找算法 完成 """
    def position(kw, context):
        """获取关键词在文本中的位置"""
        positions, p = [], 0
        while 1:
            p = context.find(kw, p)
            if p == -1:
                break
            else:
                positions.append(p)
                p += 1

        return positions

    positions = {}
    for kw in kws:
        positions[kw] = position(kw, context)

    return positions


@time_analyze
def kmp_algorithm(kws, context):
    """ 使用 KMP 字符串查找算法 完成 """
    class KMP:
        """ KMP 算法 """

        def partial(self, pattern):
            """ Calculate partial match table: String -> [Int]"""
            ret = [0]

            for i in range(1, len(pattern)):
                j = ret[i - 1]
                while j > 0 and pattern[j] != pattern[i]:
                    j = ret[j - 1]
                ret.append(j + 1 if pattern[j] == pattern[i] else j)
            return ret

        def search(self, T, P):
            """
            KMP search main algorithm: String -> String -> [Int]
            Return all the matching position of pattern string P in S
            """
            partial, ret, j = self.partial(P), [], 0

            for i in range(len(T)):
                while j > 0 and T[i] != P[j]:
                    j = partial[j - 1]
                if T[i] == P[j]:
                    j += 1
                if j == len(P):
                    ret.append(i - (j - 1))
                    j = 0

            return ret

    positions = {}
    for kw in kws:
        positions[kw] = KMP().search(context, kw)

    return positions


@time_analyze
def navie_algorithm(kws, context):
    """ 朴素算法 """
    def matcher(t, p):
        """
        :param t: the string to check
        :param p: pattern
        """
        r, n, m = [], len(t), len(p)
        for s in xrange(0, n - m + 1):
            if p == t[s:s + m]:
                r.append(s)

        return r

    positions = {}
    for kw in kws:
        positions[kw] = matcher(context, kw)

    return positions


@time_analyze
def rabin_karp_algorithm(kws, context):
    """ Rabin-Karp 算法 """
    class RollingHash:

        def __init__(self, string, size):
            self.str = string
            self.hash = 0

            for i in xrange(0, size):
                self.hash += ord(self.str[i])

            self.init = 0
            self.end = size

        def update(self):
            if self.end <= len(self.str) - 1:
                self.hash -= ord(self.str[self.init])
                self.hash += ord(self.str[self.end])
                self.init += 1
                self.end += 1

        def digest(self):
            return self.hash

        def text(self):
            return self.str[self.init:self.end]

    def matcher(string, substring):
        if substring == None or string == None:
            return -1

        if substring == "" or string == "":
            return -1

        if len(substring) > len(string):
            return -1

        hs = RollingHash(string, len(substring))
        hsub = RollingHash(substring, len(substring))
        hsub.update()

        r = []
        for i in range(len(string) - len(substring) + 1):
            if hs.digest() == hsub.digest():
                if hs.text() == substring:
                    r.append(i)

            hs.update()

        return r

    positions = {}
    for kw in kws:
        positions[kw] = matcher(context, kw)

    return positions


def keyword_tree(kws):
    """ 根据关键词生成关键词树 """
    tree = ahocorasick.Automaton()
    for i, kw in enumerate(kws):
        tree.add_word(kw, (i, kw))

    tree.make_automaton()
    return tree


@time_analyze
def ahocorasick_algorithm(kws, context):
    """
    有限自动机算法

    算法有点问题结果不对差几位，需要调整。
    """
    tree = keyword_tree(kws)
    positions = {}
    for item in tree.iter(context):
        (p, (i, kw)) = item
        if kw not in positions:
            positions[kw] = [p]
        else:
            positions[kw].append(p)

    return positions


@time_analyze
def ahocorasick_algorithm_by_sentence(kws, context):
    """
    有限自动机算法（按照每局循环）
    """
    tree = keyword_tree(kws)
    sentences = context.split("。")
    positions = {}
    for sentence in sentences:
        for item in tree.iter(sentence):
            (p, (i, kw)) = item
            if kw not in positions:
                positions[kw] = [p]
            else:
                positions[kw].append(p)

    return positions


def main():
    """ 主函数 """
    context = """
        通过文字创造出想象的现实，就能让大批互不相识的人有效合作，而且效果还不只如此。正由于大规模的人类合作是以虚构的故事作为基
        础，只要改变所讲的故事，就能改变人类合作的方式。只要在对的情境之下，这些故事就能迅速改变。例如在1789年，法国人几乎是在一夕之
        间，相信的故事就从“天赋君权”转成“人民做主”。因此，自从认知革命之后，智人就能依据不断变化的需求迅速调整行为。这等于开启了一条采
        用“文化演化”的快速道路，而不再停留在“基因演化”这条总是堵车的道路上。走上这条快速道路之后，智人合作的能力一日千里，很快就远远甩
        掉了其他所有人类和动物物种。
        其他同样具有社会行为的动物，它们的行为有相当程度都是出于基因。但DNA并不是唯一的决定因素，其他因素还包括环境影响以及个体
        的特殊之处。然而，在特定的环境中，同一物种的动物也倾向表现出类似的行为模式。一般来说，如果没有发生基因突变，它们的社会行为就
        不会有显著的改变。举例来说，黑猩猩天生就会形成阶层井然的团体，由某个雄性首领领导。然而，倭黑猩猩（bonobo，与黑猩猩极为相似）
        的团体就较为平等，而且通常由雌性担任首领。雌黑猩猩无法向倭黑猩猩这种算是近亲的物种学习，发动一场女权主义革命。相较之下，雄性
        黑猩猩也不可能召开猩民大会推翻首领，再宣布从现在起所有黑猩猩生而平等。像这样的剧烈改变，对黑猩猩来说就只有DNA改变才可能发
        生。
        出于类似的原因，远古人类也没有什么革命性的改变。据我们所知，过去想要改变社会结构、发明新科技或是移居到新的地点，多半是因
        为基因突变、环境压力，而不常是因为文化的理由。正因如此，人类才得花上几十万年走到这一步。两百万年前，就是因为基因突变，才让“直
        立人”这种新的人类物种出现。而直立人出现后，也发展出新的石器技术，现在公认为是这个物种的定义特征。而只要直立人没有进一步的基因
        改变，他们的石器也就维持不变，就这样过了两百万年！
        与此相反的是，在认知革命之后，虽然智人的基因和环境都没什么改变，但智人还是能够迅速改变行为，并将新的行为方式传给下一代。
        最典型的例子，就是人类社会总会出现不生育的精英阶层，像是天主教的神父、佛教的高僧，还有中国的太监。这些精英阶层虽然手中握有权
        力，但却自愿放弃生育，于是他们的存在根本就直接抵触了自然选择的最大原则。看看黑猩猩，它们的雄性首领会无所不用其极，尽可能和所
        有母猩猩交配，这样才能让群体中多数的年轻猩猩都归自己所有——但天主教的首领却是选择完全禁欲、无子无女。而且，他们禁欲并不是因
        为环境因素，像是严重缺乏食物、严重缺少对象等等，也不是因为有了什么古怪的基因突变。天主教会至今已存在上千年，它靠的不是把什
        么“禁欲基因”从这个教宗传到下一个教宗，而是靠着把《圣经·新约》和教律所营造出的故事代代相传。
        换句话说，过去远古人类的行为模式可能维持几万年不变，但对现代智人来说，只要十几二十年，就可能改变整个社会结构、人际交往关
        系和经济活动。像是有一位曾住在柏林的老太太，她出生于1900年，总共活了100岁。她童年的时候，是活在腓特烈·威廉二世的霍亨佐伦帝国
        （Hohenzollern Empire）；等她成年，还经历了魏玛共和国、纳粹德国，还有民主德国（东德）；等到她过世的时候，则是统一后的德国的公
        民。虽然她的基因从未改变，她却经历过了五种非常不同的社会政治制度。
        这正是智人成功的关键。如果是一对一单挑，尼安德特人应该能把智人揍扁。但如果是上百人的对立，尼安德特人就绝无获胜的可能。尼
        安德特人虽然能够分享关于狮子在哪的信息，却大概没办法传颂（和改写）关于部落守护灵的故事。而一旦没有这种建构虚幻故事的能力，尼
        安德特人就无法有效大规模合作，也就无法因应快速改变的挑战，调整社会行为。
        虽然我们没办法进到尼安德特人的脑子里，搞清楚他们的思考方式，但我们还是有些间接证据，证明他们和竞争对手智人之间的认知能力
        差异与极限。考古学家在欧洲内陆挖掘3万年前的智人遗址，有时候会发现来自地中海和大西洋沿岸的贝壳。几乎可以确定，这些贝壳是因为不
        同智人部落之间的远距贸易，才传到了大陆内部。然而，尼安德特人的遗址就找不到任何此类贸易的证据，每个部落都只用自己当地的材料，
        制造出自己的工具。
        另一个例子来自南太平洋。在新几内亚以北的新爱尔兰岛曾经住着一些智人，他们会使用一种叫作黑曜石的火山晶体，制造出特别坚硬且
        尖锐的工具。然而，新爱尔兰岛其实并不产黑曜石。化验结果显示，他们用的黑曜石来自超过400公里远的新不列颠岛。所以，这些岛上一定
        有某些居民是老练的水手，能够进行长距离的岛对岛交易。
        乍看之下，可能觉得贸易这件事再实际不过，并不需要什么虚构的故事当作基础。然而，事实就是所有动物只有智人能够进行贸易，而所
        有我们有详细证据证明存在的贸易网络都明显以虚构故事为基础。例如，如果没有信任，就不可能有贸易，而要相信陌生人又是件很困难的
        事。今天之所以能有全球贸易网络，正是因为我们相信着一些虚拟实体，像是美元、联邦储备银行，还有企业的商标。而在部落社会里，如果
        两个陌生人想要交易，往往也得先借助共同的神明、传说中的祖先或图腾动物建立信任。
        如果相信这些事的远古智人要交易贝壳和黑曜石，顺道交易一些信息应该也十分合理；这样一来，比起尼安德特人或其他远古人类物种，
        智人就有了更深更广的知识。
        从狩猎技术也能够看出尼安德特人和智人的差异。尼安德特人狩猎时通常是独自出猎，或是只有一小群人合作。但另一方面，智人就发展
        出了需要几十个人甚至不同部落合作的狩猎技巧。一种特别有效的方法，就是将野马之类的整个动物群给围起来，赶进某个狭窄的峡谷，这样
        很容易一网打尽。如果一切计划顺利进行，只要合作一个下午，这几个部落就能得到上吨的鲜肉、脂肪和兽皮，除了可以饱食一顿，也可以风
        干、烟熏或冰冻，留待日后使用。考古学家已经发现多处遗址，都曾用这种方式屠杀了整个兽群。甚至还有遗址发现了栅栏和障碍物，作为陷
        阱和屠宰场之用。
        我们可以想象，尼安德特人看到自己过去的猎场成了受智人控制的屠宰场，心里应该很不是滋味。然而，一旦这两个物种发生冲突，尼安
        德特人的情势可能不比野马好到哪去。尼安德特人可能会用他们传统的方式来合作，集结50人前往攻击智人，但创新而又灵活的智人却能集结
        起500人来同心协力，于是输赢早已预定。而且，就算智人输了第一战，他们也会快速找出新的策略，在下一战讨回来。
    """

    kws = ["智人", "尼安德特人", "基因", "认知能力", "改变", "黑猩猩"]

    kws = []
    for kw in jieba.cut(context):
        kws.append(kw.encode("utf-8"))

    # print kws
    common_algorithm(kws, context)
    kmp_algorithm(kws, context)
    navie_algorithm(kws, context)
    rabin_karp_algorithm(kws, context)
    ahocorasick_algorithm(kws, context)


if __name__ == "__main__":
    main()
