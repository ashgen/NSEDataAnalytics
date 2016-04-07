import feedparser

RSS=["http://economictimes.indiatimes.com/rssfeeds/12872390.cms",
     "http://economictimes.indiatimes.com/rssfeeds/2146842.cms",
     "http://economictimes.indiatimes.com/rssfeeds/1808152121.cms",
     "http://www.moneycontrol.com/rss/MCtopnews.xml",
     "http://www.moneycontrol.com/rss/buzzingstocks.xml",
     ]

if __name__=='__main__':
    for r in RSS:
        feed=feedparser.parse(r)
        for f in feed.entries:
            print f.title
            