from multiprocessing import Pool


def seg_test(sentence=None, _type=None, n=None):
    import jieba
    print("{0} - {1}".format(n, id(jieba)))


if __name__ == "__main__":
    n = 10
    pool = Pool(processes=n)
    for i in range(0, n):
        pool.apply_async(seg_test, args=(1, 1, i))

    pool.close()
    pool.join()
