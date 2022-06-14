package io.awspring.cloud.messaging.support;

import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.config.BeanExpressionContext;
import org.springframework.beans.factory.config.BeanExpressionResolver;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.context.expression.StandardBeanExpressionResolver;
import org.springframework.util.StringUtils;

/**
 * Class with convenient expression resolving methods.
 *
 * @author Tomaz Fernandes
 * @since 3.0
 */
public class ExpressionResolvingHelper implements BeanFactoryAware {

	private static final String THE_LEFT = "The [";

	private static final String RESOLVED_TO_LEFT = "Resolved to [";

	private static final String RIGHT_FOR_LEFT = "] for [";

	private BeanFactory beanFactory;

	private BeanExpressionResolver resolver = new StandardBeanExpressionResolver();

	private BeanExpressionContext expressionContext;

	public String asString(String value, String attribute) {
		if (!StringUtils.hasText(value)) {
			return null;
		}
		Object resolved = resolveExpression(value);
		if (resolved instanceof String) {
			return (String) resolved;
		}
		else if (resolved != null) {
			throw new IllegalStateException(THE_LEFT + attribute + "] must resolve to a String. "
				+ RESOLVED_TO_LEFT + resolved.getClass() + RIGHT_FOR_LEFT + value + "]");
		}
		return null;
	}

	public Integer asInteger(String value, String attribute) {
		if (!StringUtils.hasText(value)) {
			return null;
		}
		Object resolved = resolveExpression(value);
		if (resolved instanceof Integer) {
			return (Integer) resolved;
		} else if (resolved instanceof String) {
			return Integer.parseInt((String) resolved);
		}
		else if (resolved != null) {
			throw new IllegalStateException(THE_LEFT + attribute + "] must resolve to Integer. "
				+ RESOLVED_TO_LEFT + resolved.getClass() + RIGHT_FOR_LEFT + value + "]");
		}
		return null;
	}

	private Object resolveExpression(String value) {
		return this.resolver.evaluate(resolve(value), this.expressionContext);
	}

	private String resolve(String value) {
		if (this.beanFactory != null && this.beanFactory instanceof ConfigurableBeanFactory) {
			return ((ConfigurableBeanFactory) this.beanFactory).resolveEmbeddedValue(value);
		}
		return value;
	}

	public void setBeanFactory(BeanFactory beanFactory) {
		this.beanFactory = beanFactory;
		if (beanFactory instanceof ConfigurableListableBeanFactory) {
			this.resolver = ((ConfigurableListableBeanFactory) beanFactory).getBeanExpressionResolver();
			this.expressionContext = new BeanExpressionContext((ConfigurableListableBeanFactory) beanFactory,
				null);
		}
	}

}
