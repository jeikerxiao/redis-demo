import redis.clients.jedis.Jedis;
import redis.clients.jedis.ZParams;

import java.util.*;

public class Chapter01 {

    private static final int ONE_WEEK_IN_SECONDS = 7 * 86400;
    private static final int VOTE_SCORE = 432;
    private static final int ARTICLES_PER_PAGE = 25;

    public static final void main(String[] args) {
        new Chapter01().run();
    }

    public void run() {
        // redis 连接
        Jedis conn = new Jedis("192.168.99.100", 32768);
        // redis 数据库
        conn.select(1);
        // 发布文章
        String articleId = postArticle(conn, "username", "A title", "http://www.google.com");
        // 打印发布的文章
        System.out.println("We posted a new article with id: " + articleId);
        System.out.println("Its HASH looks like:");
        Map<String, String> articleData = conn.hgetAll("article:" + articleId);
        for (Map.Entry<String, String> entry : articleData.entrySet()) {
            System.out.println("  " + entry.getKey() + ": " + entry.getValue());
        }

        System.out.println();
        // 投票
        articleVote(conn, "other_user", "article:" + articleId);
        String votes = conn.hget("article:" + articleId, "votes");
        System.out.println("We voted for the article, it now has votes: " + votes);
        assert Integer.parseInt(votes) < 1;
        // 获取最高评分的文章
        System.out.println("The currently highest-scoring articles are:");
        List<Map<String, String>> articles = getArticles(conn, 1);
        printArticles(articles);
        assert articles.size() >= 1;
        // 添加文章到分组
        addGroups(conn, articleId, new String[]{"new-group"});
        System.out.println("We added the article to a new group, other articles include:");
        // 获取整页分组的文章
        articles = getGroupArticles(conn, "new-group", 1);
        printArticles(articles);
        assert articles.size() >= 1;
    }

    /**
     * 发布文章
     *
     * @param conn
     * @param user  用户
     * @param title 文章标题
     * @param link  文章连接
     * @return
     */
    public String postArticle(Jedis conn, String user, String title, String link) {
        // 生成文章id
        String articleId = String.valueOf(conn.incr("article:"));
        // 将文章作者添加到文章已投票文章中（自己不能为自己投票）
        // 然后将这个投票截止时间设置为一周
        String voted = "voted:" + articleId;
        conn.sadd(voted, user);
        conn.expire(voted, ONE_WEEK_IN_SECONDS);
        // 将文章信息存储到一个hash中
        long now = System.currentTimeMillis() / 1000;
        String article = "article:" + articleId;
        HashMap<String, String> articleData = new HashMap<>();
        articleData.put("title", title);
        articleData.put("link", link);
        articleData.put("user", user);
        articleData.put("now", String.valueOf(now));
        articleData.put("votes", "1");
        conn.hmset(article, articleData);
        // 将文章添加到：按评分排序的文章列表中
        conn.zadd("score:", now + VOTE_SCORE, article);
        // 将文章添加到：按时间排序的文章列表中
        conn.zadd("time:", now, article);

        return articleId;
    }

    /**
     * 为文章投票功能
     *
     * @param conn
     * @param user    投票的用户
     * @param article 被投票的文章
     */
    public void articleVote(Jedis conn, String user, String article) {
        // 计算文章的投票截止时间
        long cutoff = (System.currentTimeMillis() / 1000) - ONE_WEEK_IN_SECONDS;
        // 如果已经过了投票截止时间，则不可以投票了
        if (conn.zscore("time:", article) < cutoff) {
            return;
        }
        // 获取文章id
        String articleId = article.substring(article.indexOf(':') + 1);
        // 如果用户是第一次为此文章投票，则增加文章评分、投票数量
        if (conn.sadd("voted:" + articleId, user) == 1) {
            conn.zincrby("score:", VOTE_SCORE, article);
            conn.hincrBy(article, "votes", 1);
        }
    }

    /**
     * 获取文章(根据评分排序)
     *
     * @param conn
     * @param page
     * @return
     */
    public List<Map<String, String>> getArticles(Jedis conn, int page) {
        return getArticles(conn, page, "score:");
    }

    /**
     * 获取文章(可选择排序项)
     *
     * @param conn
     * @param page
     * @return
     */
    public List<Map<String, String>> getArticles(Jedis conn, int page, String order) {
        int start = (page - 1) * ARTICLES_PER_PAGE;
        int end = start + ARTICLES_PER_PAGE - 1;

        Set<String> ids = conn.zrevrange(order, start, end);
        List<Map<String, String>> articles = new ArrayList<>();
        for (String id : ids) {
            Map<String, String> articleData = conn.hgetAll(id);
            articleData.put("id", id);
            articles.add(articleData);
        }

        return articles;
    }

    /**
     * 添加文章到分组中
     *
     * @param conn
     * @param articleId
     * @param toAdd
     */
    public void addGroups(Jedis conn, String articleId, String[] toAdd) {
        String article = "article:" + articleId;
        for (String group : toAdd) {
            conn.sadd("group:" + group, article);
        }
    }

    /**
     * 获取整页的文章（根据评分排序）
     *
     * @param conn
     * @param group
     * @param page
     * @return
     */
    public List<Map<String, String>> getGroupArticles(Jedis conn, String group, int page) {
        return getGroupArticles(conn, group, page, "score:");
    }

    /**
     * 获取整页的文章（可选择排序项）
     *
     * @param conn
     * @param group
     * @param page
     * @param order
     * @return
     */
    public List<Map<String, String>> getGroupArticles(Jedis conn, String group, int page, String order) {
        String key = order + group;
        if (!conn.exists(key)) {
            ZParams params = new ZParams().aggregate(ZParams.Aggregate.MAX);
            conn.zinterstore(key, params, "group:" + group, order);
            conn.expire(key, 60);
        }
        return getArticles(conn, page, key);
    }

    /**
     * 打印文章
     *
     * @param articles
     */
    private void printArticles(List<Map<String, String>> articles) {
        for (Map<String, String> article : articles) {
            System.out.println("  id: " + article.get("id"));
            for (Map.Entry<String, String> entry : article.entrySet()) {
                if (entry.getKey().equals("id")) {
                    continue;
                }
                System.out.println("    " + entry.getKey() + ": " + entry.getValue());
            }
        }
    }
}
