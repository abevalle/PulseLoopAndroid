# kotlinx.serialization — keep generated serializers so JSON parsing survives R8.
# (Modern kotlinx-serialization ships consumer rules, but these make it explicit.)
-keepattributes RuntimeVisibleAnnotations,AnnotationDefault

-if @kotlinx.serialization.Serializable class **
-keepclassmembers class <1> {
    static <1>$Companion Companion;
}
-if @kotlinx.serialization.Serializable class ** {
    static **$* *;
}
-keepclassmembers class <2>$<3> {
    kotlinx.serialization.KSerializer serializer(...);
}
-if @kotlinx.serialization.Serializable class ** {
    public static ** INSTANCE;
}
-keepclassmembers class <1> {
    public static <1> INSTANCE;
    kotlinx.serialization.KSerializer serializer(...);
}

# Self-update models parsed from the GitHub releases API.
-keep,includedescriptorclasses class com.pulseloop.update.**$$serializer { *; }
-keepclassmembers class com.pulseloop.update.** {
    *** Companion;
}
-keepclasseswithmembers class com.pulseloop.update.** {
    kotlinx.serialization.KSerializer serializer(...);
}

# Fork: HiveMQ MQTT client (export/mqtt). Netty and JCTools reach fields reflectively by name
# (HiveMQ's Android guidance); the dontwarns cover optional Netty integrations Android lacks.
-keepclassmembernames class io.netty.** { *; }
-keepclassmembers class org.jctools.** { *; }
-dontwarn io.netty.**
-dontwarn org.jctools.**
-dontwarn reactor.blockhound.**
