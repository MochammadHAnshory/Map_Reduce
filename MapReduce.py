import sys
sys.path.insert(0, r'c:\users\fnofliwar\appdata\local\programs\python\python310-32\lib\site-packages')
from mrjob.job import MRJob
import re

WORD_RE = re.compile (r"['\w']+")

class MRWordFreqCount(MRJob) :
    def mapper (self, _, line) :
        for word in WORD_RE.findall(line) :
            yield (word.lower(), 1)
    
    def reducer (self, word, counts) :
        yield (word, sum(counts))


if __name__ == '__main__' :
   
    # print(MRWordFreqCount._print_help())
    MRWordFreqCount.run()
    