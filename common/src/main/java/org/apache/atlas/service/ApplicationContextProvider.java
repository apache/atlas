package org.apache.atlas.service;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

@Component
public class ApplicationContextProvider implements ApplicationContextAware {
    
    private static ApplicationContext context;
    
    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        context = applicationContext;
    }
    
    public static <T> T getBean(Class<T> beanClass) {
        if (context == null) {
            return null;
        }
        try {
            return context.getBean(beanClass);
        } catch (BeansException e) {
            return null;
        }
    }
    
    public static <T> T getBean(String beanName, Class<T> beanClass) {
        if (context == null) {
            return null;
        }
        try {
            return context.getBean(beanName, beanClass);
        } catch (BeansException e) {
            return null;
        }
    }
    
    public static ApplicationContext getApplicationContext() {
        return context;
    }
}
