package org.awsutils.sqs.aspects;

import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.DeclareParents;

@Aspect
public class MessageHandlerAspect {
    @DeclareParents(value = "org.awsutils..SqsMessageHandler+", defaultImpl = SqsMessageSenderInjectorImpl.class)
    private SqsMessageSenderInjector implementedInterface;
}
