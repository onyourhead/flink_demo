package skeleton;

/**
 * @author 张政淇
 * @class UserBehavior
 * @desc 此类为POJO类，以便flink进行序列化和反序列化，
 * 一个类要成为POJO类要求其字段要么全部为public，要么提供所有字段的get和set函数，这里选择后者做实现
 * @date 2020/1/15 14:56
 */
public class UserBehavior {
    /**
     * 用户ID
     */
    private long userId;

    /**
     * 商品ID
     */
    private long itemId;

    /**
     * 商品类目ID
     */
    private int categoryId;

    /**
     * 用户行为, 包括("pv", "buy", "cart", "fav")
     */
    private String behavior;

    /**
     * 行为发生的时间戳，单位秒
     */
    private long timestamp;

    /**
     * POJO类需要有一个无参构造函数
     */
    public UserBehavior() {
    }

    public UserBehavior(long userId, long itemId, int categoryId, String behavior, long timestamp) {
        this.userId = userId;
        this.itemId = itemId;
        this.categoryId = categoryId;
        this.behavior = behavior;
        this.timestamp = timestamp;
    }

    public long getUserId() {
        return userId;
    }

    public void setUserId(long userId) {
        this.userId = userId;
    }

    public long getItemId() {
        return itemId;
    }

    public void setItemId(long itemId) {
        this.itemId = itemId;
    }

    public int getCategoryId() {
        return categoryId;
    }

    public void setCategoryId(int categoryId) {
        this.categoryId = categoryId;
    }

    public String getBehavior() {
        return behavior;
    }

    public void setBehavior(String behavior) {
        this.behavior = behavior;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
}
