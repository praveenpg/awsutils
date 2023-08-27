package org.awsutils.dynamodb.utils;

import com.google.common.collect.ImmutableMap;
import org.awsutils.common.exceptions.UtilsException;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.lang.reflect.Proxy;
import java.text.MessageFormat;
import java.util.Map;

@SuppressWarnings({"unused", "RedundantSuppression"})
class FilterExprImpl implements FilterExpr {
    public static FilterExpr getInstance() {
        return new FilterExprImpl();
    }

    private NameImpl rootName;
    private GroupImpl rootGroup;

    @Override
    public Name name(final String name, final String alias) {
        this.rootName = new NameImpl(name, alias, this);

        return this.rootName;
    }

    @Override
    public Group group(final Expr expr) {
        return (this.rootGroup = new GroupImpl(this, expr));
    }

    @Override
    public Name name(final String name) {
        return name(name, name);
    }


    private static final class ExprImpl implements Expr {
        private final NameImpl rootNode;
        private final GroupImpl rootGroup;

        private ExprImpl(FilterExprImpl filterExpr) {
            this.rootNode = filterExpr.rootName;
            this.rootGroup = filterExpr.rootGroup;
        }

        @Override
        public String expression() {
            if (rootNode != null) {
                return rootNode.expression();
            } else if (rootGroup != null) {
                return rootGroup.expression();
            } else {
                throw new UtilsException("Invalid Expression");
            }
        }

        @Override
        public Map<String, String> attributeNameMap() {
            if (rootNode != null) {
                return rootNode.attributeNameMap();
            } else if (rootGroup != null) {
                return rootGroup.attributeNameMap();
            } else {
                throw new UtilsException("Invalid Expression");
            }
        }

        @Override
        public Map<String, AttributeValue> attributeValueMap() {
            if (rootNode != null) {
                return rootNode.attributeValueMap();
            } else if (rootGroup != null) {
                return rootGroup.attributeValueMap();
            } else {
                throw new UtilsException("Invalid Expression");
            }
        }
    }

    private static class NameImpl implements Name {
        private final String name;
        private final String alias;
        private final FilterExprImpl filterExpression;
        private AbstractComparator comparator;

        public NameImpl(final String name, final String alias, final FilterExprImpl filterExpression) {
            this.name = name;
            this.alias = alias;
            this.filterExpression = filterExpression;
        }

        @Override
        public SingleValueComparator gt() {
            return (SingleValueComparator) (this.comparator = new GreaterThan(filterExpression));
        }

        @Override
        public SingleValueComparator lt() {
            return (SingleValueComparator) (this.comparator = new LessThan(filterExpression));
        }

        @Override
        public SingleValueComparator gte() {
            return (SingleValueComparator) (this.comparator = new GreaterThanOrEquals(filterExpression));
        }

        @Override
        public SingleValueComparator lte() {
            return (SingleValueComparator) (this.comparator = new LessThanOrEquals(filterExpression));
        }

        @Override
        public SingleValueComparator eq() {
            return (SingleValueComparator) (this.comparator = new Equals(filterExpression));
        }

        @Override
        public SingleValueComparator ne() {
            return (SingleValueComparator) (this.comparator = new NotEquals(filterExpression));
        }


        @Override
        public SingleValueComparator notExists() {
            return (SingleValueComparator) (this.comparator = new NotExists(filterExpression));
        }

        public DoubleValueComparator between() {
            return (DoubleValueComparator) (this.comparator = new Between(filterExpression));
        }

        String expression() {
            if (!(comparator instanceof NotExists) && !(comparator instanceof Between)) {
                final SingleValueImpl value = (SingleValueImpl) comparator.value;
                final Operator operator;

                if (value == null) {
                    throw new UtilsException("Invalid Expression");
                }

                operator = value.operator();

                return MessageFormat.format("#{0}{1}:{2}{3}", alias, comparator.expression(), value.name, operator == null ? "" : operator.expression());
            } else if(comparator instanceof Between){
                final DoubleValueImpl value = (DoubleValueImpl) comparator.value;
                final Operator operator;


                if(value.value1 == null || value.value2 == null) {
                    throw new UtilsException("Invalid Expression");
                }

                operator = value.operator();

                return MessageFormat.format("#{0}{1}:{2} and :{3} {4}", alias, comparator.expression(), value.value1.name, value.value2.name,
                        operator == null ? "" : operator.expression() + " ");
            } else {
                return MessageFormat.format(comparator.expression(), name);
            }
        }

        Map<String, String> attributeNameMap() {
            final ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();

            if (!(comparator instanceof NotExists)) {
                final AbstractValueImpl value = (AbstractValueImpl) comparator.value;
                final AbstractOperator operator = value.operator();

                builder.put("#" + alias, name);

                if(operator != null) {
                    builder.putAll(operator.attributeNameMap());
                }

                return builder.build();
            }

            return builder.build();
        }

        Map<String, AttributeValue> attributeValueMap() {
            if (!(comparator instanceof NotExists) && !(comparator instanceof Between)) {
                final ImmutableMap.Builder<String, AttributeValue> builder = ImmutableMap.builder();
                final SingleValueImpl value = (SingleValueImpl) comparator.value;
                final AbstractOperator operator = value.operator();

                builder.put(":" + value.name, value.value);

                if (operator != null) {
                    builder.putAll(operator.attributeValueMap());
                }

                return builder.build();
            } else if(comparator instanceof Between){
                final ImmutableMap.Builder<String, AttributeValue> builder = ImmutableMap.builder();
                final DoubleValueImpl value = (DoubleValueImpl) comparator.value;
                final AbstractOperator operator = value.operator();

                builder.put(":" + value.value1.name, value.value1.value);
                builder.put(":" + value.value2.name, value.value2.value);

                if (operator != null) {
                    builder.putAll(operator.attributeValueMap());
                }

                return builder.build();
            } else {
                return ImmutableMap.of();
            }
        }

        @Override
        public String toString() {
            return "Name{" +
                    "name='" + name + '\'' +
                    ", alias='" + alias + '\'' +
                    ", comparator=" + comparator +
                    '}';
        }
    }

    private static abstract class AbstractComparator implements Comparator {
        protected Value value;
        protected final FilterExprImpl filterExpression;

        public AbstractComparator(final FilterExprImpl filterExpression) {
            this.filterExpression = filterExpression;
        }

        @Override
        public String toString() {
            return "AbstractComparator1{" +
                    "value=" + value +
                    ", filterExpression=" + filterExpression +
                    '}';
        }
    }

    @SuppressWarnings("unchecked")
    private static abstract class SingleValueAbstractComparator extends AbstractComparator implements SingleValueComparator {

        protected SingleValueAbstractComparator(final FilterExprImpl filterExpression) {
            super(filterExpression);
        }

        public Value value(final String name, final AttributeValue value) {
            return this.value = new SingleValueImpl(name, value, filterExpression);
        }

        @Override
        public String toString() {
            return "AbstractComparator{} " + super.toString();
        }
    }

    @SuppressWarnings("unchecked")
    private static abstract class DoubleValueAbstractComparator extends AbstractComparator implements DoubleValueComparator {
        public DoubleValueAbstractComparator(FilterExprImpl filterExpression) {
            super(filterExpression);
        }

        @Override
        public Value value(final String name1, final AttributeValue value1, final String name2, final AttributeValue value2) {
            return (this.value = new DoubleValueImpl(new SingleValueImpl(name1, value1, filterExpression), new SingleValueImpl(name2, value2, filterExpression), filterExpression));
        }

        @Override
        public String toString() {
            return "AbstractComparator{" +
                    "value=" + value +
                    '}';
        }
    }

    private static class LessThan extends SingleValueAbstractComparator {

        public LessThan(final FilterExprImpl filterExpression) {
            super(filterExpression);
        }

        @Override
        public String expression() {
            return " < ";
        }

        @Override
        public String toString() {
            return "LessThan{} " + super.toString();
        }
    }

    private static final class Between extends DoubleValueAbstractComparator {
        public Between(final FilterExprImpl filterExpression) {
            super(filterExpression);
        }


        @Override
        public String expression() {
            return " between ";
        }

        @Override
        public String toString() {
            return "Between{} " + super.toString();
        }
    }

    private static final class GreaterThan extends SingleValueAbstractComparator {

        private GreaterThan(final FilterExprImpl filterExpression) {
            super(filterExpression);
        }

        @Override
        public String expression() {
            return " > ";
        }

        @Override
        public String toString() {
            return "GreaterThan{} " + super.toString();
        }
    }

    private static final class LessThanOrEquals extends SingleValueAbstractComparator {
        private LessThanOrEquals(final FilterExprImpl filterExpression) {
            super(filterExpression);
        }

        @Override
        public String expression() {
            return " <= ";
        }

        @Override
        public String toString() {
            return "LessThanOrEquals{} " + super.toString();
        }
    }

    private static final class GreaterThanOrEquals extends SingleValueAbstractComparator {
        private GreaterThanOrEquals(final FilterExprImpl filterExpression) {
            super(filterExpression);
        }

        @Override
        public String expression() {
            return " >= ";
        }

        @Override
        public String toString() {
            return "GreaterThanOrEquals{} " + super.toString();
        }
    }

    private static final class Equals extends SingleValueAbstractComparator {

        private Equals(final FilterExprImpl filterExpression) {
            super(filterExpression);
        }

        @Override
        public String expression() {
            return " = ";
        }

        @Override
        public String toString() {
            return "Equals{} " + super.toString();
        }
    }

    private static final class NotEquals extends SingleValueAbstractComparator {

        private NotEquals(final FilterExprImpl filterExpression) {
            super(filterExpression);
        }

        @Override
        public String expression() {
            return " <> ";
        }

        @Override
        public String toString() {
            return "NotEquals{} " + super.toString();
        }
    }

    private static final class NotExists extends SingleValueAbstractComparator {
        public NotExists(final FilterExprImpl filterExpression) {
            super(filterExpression);
        }

        public Value value(final String name, final AttributeValue value) {
            throw new UnsupportedOperationException("This is not supported for a \"Not exists\" check");
        }

        @Override
        public String expression() {
            return "attribute_not_exists({0})";
        }
    }

    private static abstract class AbstractValueImpl implements Value {
        private AbstractOperator operator;
        private final FilterExprImpl filterExpression;

        private AbstractValueImpl(final FilterExprImpl filterExpression) {
            this.filterExpression = filterExpression;
        }

        @Override
        public Name and(final String name, final String alias) {
            this.operator = new And(name, alias, filterExpression);

            return this.operator.nextName;
        }

        @Override
        public Operator and() {
            return (this.operator = new And(filterExpression));
        }

        @Override
        public Name and(final String name) {
            return and(name, name);
        }

        @Override
        public Name or(final String name, final String alias) {
            this.operator = new Or(name, alias, filterExpression);

            return this.operator.nextName;
        }

        @Override
        public Operator or() {
            return (this.operator = new Or(filterExpression));
        }

        @Override
        public Name or(final String name) {
            return or(name, name);
        }

        @Override
        public Expr buildFilterExpression() {
            return new ExprImpl(filterExpression);
        }

        public AbstractOperator operator() {
            return operator;
        }

        @Override
        public String toString() {
            return "AbstractValueImpl{" +
                    "operator=" + operator +
                    ", filterExpression=" + filterExpression +
                    '}';
        }
    }

    private static final class DoubleValueImpl extends AbstractValueImpl {
        private final SingleValueImpl value1;
        private final SingleValueImpl value2;

        public DoubleValueImpl(SingleValueImpl value1, SingleValueImpl value2, FilterExprImpl filterExpression) {
            super(filterExpression);
            this.value1 = value1;
            this.value2 = value2;
        }

        @Override
        public String toString() {
            return "DoubleValueImpl{" +
                    "value1=" + value1 +
                    ", value2=" + value2 +
                    "} " + super.toString();
        }
    }

    private static final class SingleValueImpl extends AbstractValueImpl {
        private final AttributeValue value;
        private final String name;


        private SingleValueImpl(final String name, final AttributeValue value, final FilterExprImpl filterExpression) {
            super(filterExpression);
            this.name = name;
            this.value = value;
        }


        @Override
        public String toString() {
            return "ValueImpl{" +
                    "value=" + value +
                    ", name='" + name + '\'' +
                    "} " + super.toString();
        }
    }

    private static abstract class AbstractOperator implements Operator {
        private NameImpl nextName;
        private final FilterExprImpl filterExpr;
        private GroupImpl group;

        protected AbstractOperator(final String nextName, final String alias, final FilterExprImpl filterExpression) {
            this.nextName = new NameImpl(nextName, alias, filterExpression);
            this.filterExpr = filterExpression;
        }

        protected AbstractOperator(final FilterExprImpl filterExpression) {
            this.filterExpr = filterExpression;
        }

        public String expression() {
            if (nextName == null && group == null) {
                throw new UtilsException("Invalid expression.");
            }

            return getExpression() + (nextName != null ? nextName.expression() : group.expression());
        }

        protected abstract String getExpression();


        @Override
        public String toString() {
            return "AbstractOperator{" +
                    "nextName=" + nextName +
                    '}';
        }


        @Override
        public Name name(final String name, final String alias) {
            return (this.nextName = new NameImpl(name, alias, filterExpr));
        }

        @Override
        public Group group(final Expr expr) {
            return (this.group = new GroupImpl(filterExpr, expr));
        }

        public Map<String, String> attributeNameMap() {
            return nextName != null ? nextName.attributeNameMap() : group.attributeNameMap();
        }

        public Map<String, AttributeValue> attributeValueMap() {
            return nextName != null ? nextName.attributeValueMap() : group.attributeValueMap();
        }
    }

    private static final class And extends AbstractOperator {

        private And(final String name, final String alias, final FilterExprImpl filterExpression) {
            super(name, alias, filterExpression);
        }

        public And(final FilterExprImpl filterExpression) {
            super(filterExpression);
        }

        @Override
        protected String getExpression() {
            return " and ";
        }
    }

    private static final class Or extends AbstractOperator {

        private Or(final String name, final String alias, final FilterExprImpl filterExpression) {
            super(name, alias, filterExpression);
        }

        public Or(final FilterExprImpl filterExpression) {
            super(filterExpression);
        }

        @Override
        public String toString() {
            return "Or{} " + super.toString();
        }

        @Override
        protected String getExpression() {
            return " or ";
        }
    }

    private static final class GroupImpl implements Group {
        private AbstractOperator operator;
        private final FilterExprImpl filterExpr;
        private final Expr expr;

        private GroupImpl(final FilterExprImpl filterExpr, final Expr expr) {
            this.filterExpr = filterExpr;
            this.expr = expr;
        }

        @Override
        public Operator and() {
            return (this.operator = new And(filterExpr));
        }

        @Override
        public Operator or() {
            return (this.operator = new Or(filterExpr));
        }

        @Override
        public String expression() {
            return operator != null ?
                    MessageFormat.format("({0}) {1}", expr.expression(), operator.expression())
                    : MessageFormat.format("({0})", expr.expression());
        }

        public Map<String, String> attributeNameMap() {
            final ImmutableMap.Builder<String, String> builder = ImmutableMap.<String, String>builder().putAll(expr.attributeNameMap());

            if (operator != null) {
                builder.putAll(operator.attributeNameMap());
            }

            return builder.build();
        }

        public Map<String, AttributeValue> attributeValueMap() {
            final ImmutableMap.Builder<String, AttributeValue> builder = ImmutableMap.<String, AttributeValue>builder().putAll(expr.attributeValueMap());

            if (operator != null) {
                builder.putAll(operator.attributeValueMap());
            }

            return builder.build();
        }

        @Override
        public Expr buildFilterExpression() {
            final Expr expr;

            if (operator != null) {
                throw new UtilsException("Invalid expression.");
            }

            expr = new ExprImpl(this.filterExpr);

            return (Expr) Proxy.newProxyInstance(this.getClass().getClassLoader(), new Class[]{Expr.class},
                    (proxy, method, args) -> method.invoke(expr, args));
        }
    }
}
