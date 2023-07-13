/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.dubbo.rpc.filter;

import org.apache.dubbo.common.beanutil.JavaBeanAccessor;
import org.apache.dubbo.common.beanutil.JavaBeanDescriptor;
import org.apache.dubbo.common.beanutil.JavaBeanSerializeUtil;
import org.apache.dubbo.common.constants.CommonConstants;
import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.PojoUtils;
import org.apache.dubbo.common.utils.ReflectUtils;
import org.apache.dubbo.rpc.Constants;
import org.apache.dubbo.rpc.Filter;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.RpcInvocation;
import org.apache.dubbo.rpc.service.GenericService;
import org.apache.dubbo.rpc.support.ProtocolUtils;
import org.apache.dubbo.rpc.support.RpcUtils;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Type;

import static org.apache.dubbo.common.constants.CommonConstants.$INVOKE;
import static org.apache.dubbo.common.constants.CommonConstants.$INVOKE_ASYNC;
import static org.apache.dubbo.rpc.Constants.GENERIC_KEY;

/**
 * GenericImplInvokerFilter
 * consumer消费端的过滤器
 * 主要是对 泛化引用 和 泛化实现 在consumer消费端的处理
 */
@Activate(group = CommonConstants.CONSUMER, value = GENERIC_KEY, order = 20000)
public class GenericImplFilter implements Filter, Filter.Listener {

    private static final Logger logger = LoggerFactory.getLogger(GenericImplFilter.class);

    /**
     * 泛化参数类型
     */
    private static final Class<?>[] GENERIC_PARAMETER_TYPES = new Class<?>[]{String.class, String[].class, Object[].class};

    private static final String GENERIC_PARAMETER_DESC = "Ljava/lang/String;[Ljava/lang/String;[Ljava/lang/Object;";

    private static final String GENERIC_IMPL_MARKER = "GENERIC_IMPL";

    @Override
    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
        // 获得 generic 配置项
        String generic = invoker.getUrl().getParameter(GENERIC_KEY);

        /**
         * calling a generic impl service
         * 1、普通consumer 调用 泛化实现 （客户端调用的服务是 GenericService）
         * 判断是否调用泛化实现
         * - generic是支持的泛化类型之一
         * - 调用方法名 不为 $invoke 且 不为$invokeAsync，即不是泛化调用
         * - 调用信息invocation 是 RpcInvocation类型
         */
        if (isCallingGenericImpl(generic, invocation)) {
            RpcInvocation invocation2 = new RpcInvocation(invocation);

            /**
             * Mark this invocation as a generic impl call, this value will be removed automatically before passing on the wire.
             * See {@link RpcUtils#sieveUnnecessaryAttachments(Invocation)}
             */
            invocation2.put(GENERIC_IMPL_MARKER, true);

            /**
             * 1.1 获取调用信息
             */
            // 调用的实际方法名
            String methodName = invocation2.getMethodName();
            // 调用的参数类型列表
            Class<?>[] parameterTypes = invocation2.getParameterTypes();
            // 调用的参数值列表
            Object[] arguments = invocation2.getArguments();

            // parameterTypes 转换为 参数类型名数组
            String[] types = new String[parameterTypes.length];
            for (int i = 0; i < parameterTypes.length; i++) {
                types[i] = ReflectUtils.getName(parameterTypes[i]);
            }

            /**
             * 1.2 根据 generic 的值选择对应序列化参数的方式
             */
            Object[] args;
            // generic == bean
            if (ProtocolUtils.isBeanGenericSerialization(generic)) {
                args = new Object[arguments.length];
                for (int i = 0; i < arguments.length; i++) {
                    // 将参数进行转换： POJO -> JavaBeanDescriptor
                    args[i] = JavaBeanSerializeUtil.serialize(arguments[i], JavaBeanAccessor.METHOD);
                }
            } else { // generic != bean
                // 将参数进行转换：POJO -> Map
                args = PojoUtils.generalize(arguments);
            }

            /**
             * 1.3 重新设置RPC调用信息，通过新的RpcInvocation就能调用到泛化实现的服务（让GenericFilter识别出要调用泛化实现）
             */
            // 设置调用方法名为 $invoke 或 $invokeAsync
            // 会覆盖真实的调用方法
            if (RpcUtils.isReturnTypeFuture(invocation)) {
                invocation2.setMethodName($INVOKE_ASYNC);
            } else {
                invocation2.setMethodName($INVOKE);
            }
            // 设置调用方法的参数类型为 泛化参数类型-GENERIC_PARAMETER_TYPES
            invocation2.setParameterTypes(GENERIC_PARAMETER_TYPES);
            invocation2.setParameterTypesDesc(GENERIC_PARAMETER_DESC);
            // 设置调用方法的参数数据，分别为 方法名、参数类型数组、参数数组
            invocation2.setArguments(new Object[]{methodName, types, args});

            /**
             * 1.4 执行调用
             */
            return invoker.invoke(invocation2);

            /**
             * 1.5 反序列化结果及异常结果处理
             * GenericImplFilter#onResponse
             */
        }
        /**
         * making a generic call to a normal service
         * 2、泛化引用的consumer 调用 普通provider （通过GenericService 调用 普通dubbo服务接口）
         * 判断是否为泛化调用
         * - 调用方法名为 $invoke 或 $invokeAsync
         * - 调用方法参数有3个
         * - generic是支持的泛化类型之一
         */
        else if (isMakingGenericCall(generic, invocation)) {
            /**
             * 2.1 获取方法参数值数组
             */
            Object[] args = (Object[]) invocation.getArguments()[2];

            /**
             * 2.2 根据 generic 的值校验参数值
             */
            // genecric = nativejava，校验方法参数是否都为 byte[]
            if (ProtocolUtils.isJavaGenericSerialization(generic)) {
                for (Object arg : args) {
                    if (!(byte[].class == arg.getClass())) {
                        error(generic, byte[].class.getName(), arg.getClass().getName());
                    }
                }
            }
            // generic = bean，校验方法参数为 JavaBeanDescriptor
            else if (ProtocolUtils.isBeanGenericSerialization(generic)) {
                for (Object arg : args) {
                    if (!(arg instanceof JavaBeanDescriptor)) {
                        error(generic, JavaBeanDescriptor.class.getName(), arg.getClass().getName());
                    }
                }
            }

            /**
             * 2.3 通过Attachment传递generic配置项，在provider端GenericFilter中反序列化会用到
             */
            invocation.setAttachment(
                    GENERIC_KEY, invoker.getUrl().getParameter(GENERIC_KEY));

            /**
             * 2.4 执行调用 invoker.invoke(invocation)
             * 2.5 反序列化结果及异常结果处理 GenericImplFilter#onResponse
             */
        }

        // 普通调用
        return invoker.invoke(invocation);
    }

    private void error(String generic, String expected, String actual) throws RpcException {
        throw new RpcException("Generic serialization [" + generic + "] only support message type " + expected + " and your message type is " + actual);
    }

    @Override
    public void onResponse(Result appResponse, Invoker<?> invoker, Invocation invocation) {
        String generic = invoker.getUrl().getParameter(GENERIC_KEY);
        String methodName = invocation.getMethodName();
        Class<?>[] parameterTypes = invocation.getParameterTypes();
        Object genericImplMarker = invocation.get(GENERIC_IMPL_MARKER);
        // 如果是调用泛化实现
        if (genericImplMarker != null && (boolean) invocation.get(GENERIC_IMPL_MARKER)) {
            // 正常结果
            if (!appResponse.hasException()) {
                // 获取调用结果
                Object value = appResponse.getValue();
                try {
                    Class<?> invokerInterface = invoker.getInterface();
                    if (!$INVOKE.equals(methodName) && !$INVOKE_ASYNC.equals(methodName)
                            && invokerInterface.isAssignableFrom(GenericService.class)) {
                        try {
                            // find the real interface from url
                            String realInterface = invoker.getUrl().getParameter(Constants.INTERFACE);
                            invokerInterface = ReflectUtils.forName(realInterface);
                        } catch (Throwable e) {
                            // ignore
                        }
                    }

                    Method method = invokerInterface.getMethod(methodName, parameterTypes);
                    if (ProtocolUtils.isBeanGenericSerialization(generic)) {
                        if (value == null) {
                            appResponse.setValue(value);
                        } else if (value instanceof JavaBeanDescriptor) {
                            appResponse.setValue(JavaBeanSerializeUtil.deserialize((JavaBeanDescriptor) value));
                        } else {
                            throw new RpcException("The type of result value is " + value.getClass().getName() + " other than " + JavaBeanDescriptor.class.getName() + ", and the result is " + value);
                        }
                    } else {
                        Type[] types = ReflectUtils.getReturnTypes(method);
                        appResponse.setValue(PojoUtils.realize(value, (Class<?>) types[0], types[1]));
                    }
                } catch (NoSuchMethodException e) {
                    throw new RpcException(e.getMessage(), e);
                }
            }
            // 异常结果
            else if (appResponse.getException() instanceof com.alibaba.dubbo.rpc.service.GenericException) {
                com.alibaba.dubbo.rpc.service.GenericException exception = (com.alibaba.dubbo.rpc.service.GenericException) appResponse.getException();
                try {
                    String className = exception.getExceptionClass();
                    Class<?> clazz = ReflectUtils.forName(className);
                    Throwable targetException = null;
                    Throwable lastException = null;
                    try {
                        targetException = (Throwable) clazz.newInstance();
                    } catch (Throwable e) {
                        lastException = e;
                        for (Constructor<?> constructor : clazz.getConstructors()) {
                            try {
                                targetException = (Throwable) constructor.newInstance(new Object[constructor.getParameterTypes().length]);
                                break;
                            } catch (Throwable e1) {
                                lastException = e1;
                            }
                        }
                    }
                    if (targetException != null) {
                        try {
                            Field field = Throwable.class.getDeclaredField("detailMessage");
                            if (!field.isAccessible()) {
                                field.setAccessible(true);
                            }
                            field.set(targetException, exception.getExceptionMessage());
                        } catch (Throwable e) {
                            logger.warn(e.getMessage(), e);
                        }
                        appResponse.setException(targetException);
                    } else if (lastException != null) {
                        throw lastException;
                    }
                } catch (Throwable e) {
                    throw new RpcException("Can not deserialize exception " + exception.getExceptionClass() + ", message: " + exception.getExceptionMessage(), e);
                }
            }
        }
    }

    @Override
    public void onError(Throwable t, Invoker<?> invoker, Invocation invocation) {

    }

    /**
     * 判断是否调用泛化实现
     * - generic是支持的泛化类型之一
     * - 调用方法名 不为 $invoke 且 不为$invokeAsync，即不是泛化调用
     * - 调用信息invocation 是 RpcInvocation类型
     * @param generic
     * @param invocation
     * @return
     */
    private boolean isCallingGenericImpl(String generic, Invocation invocation) {
        return ProtocolUtils.isGeneric(generic) // generic是支持的泛化类型之一
                // 调用方法名 不为 $invoke 且 不为$invokeAsync，即不是泛化调用
                && (!$INVOKE.equals(invocation.getMethodName()) && !$INVOKE_ASYNC.equals(invocation.getMethodName()))
                // 调用信息invocation 是 RpcInvocation类型
                && invocation instanceof RpcInvocation;
    }

    /**
     * 判断是否为泛化调用
     * 即 泛化引用的consumer 调用 普通provider
     * - 调用方法名为 $invoke 或 $invokeAsync
     * - 调用方法参数有3个
     * - generic是支持的泛化类型之一
     * @param generic
     * @param invocation
     * @return
     */
    private boolean isMakingGenericCall(String generic, Invocation invocation) {
        return (invocation.getMethodName().equals($INVOKE) || invocation.getMethodName().equals($INVOKE_ASYNC)) // 调用方法名为 $invoke 或 $invokeAsync
                // 调用方法参数有3个
                && invocation.getArguments() != null
                && invocation.getArguments().length == 3
                // generic是支持的泛化类型之一
                && ProtocolUtils.isGeneric(generic);
    }

}
