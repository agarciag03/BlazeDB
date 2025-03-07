package ed.inf.adbs.blazedb;

public class OperatorIdentifier {
    private boolean selection;
    private boolean projection;
    private boolean join;
    private boolean distinct;
    private boolean orderBy;
    private boolean groupBy;
    private boolean sum;

    public OperatorIdentifier() {
        projection = false;
        selection = false;
        join = false;
        distinct = false;
        orderBy = false;
        groupBy = false;
        sum = false;
    }

    public boolean isSelection() {
        return selection;
    }

    public void setSelection(boolean selection) {
        this.selection = selection;
    }

    public boolean isProjection() {
        return projection;
    }

    public void setProjection(boolean projection) {
        this.projection = projection;
    }

    public boolean isJoin() {
        return join;
    }

    public void setJoin(boolean join) {
        this.join = join;
    }

    public boolean isDistinct() {
        return distinct;
    }

    public void setDistinct(boolean distinct) {
        this.distinct = distinct;
    }

    public boolean isOrderBy() {
        return orderBy;
    }

    public void setOrderBy(boolean orderBy) {
        this.orderBy = orderBy;
    }

    public boolean isGroupBy() {
        return groupBy;
    }

    public void setGroupBy(boolean groupBy) {
        this.groupBy = groupBy;
    }

    public boolean isSum() {
        return sum;
    }

    public void setSum(boolean sum) {
        this.sum = sum;
    }
}
