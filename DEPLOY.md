Keen Java Clients - Deployment
==============================

## Instructions

1. Ensure that `./gradlew clean build` completes with no errors.
2. If you don't already have them, create a [Sonatype user account](https://docs.sonatype.org/display/Repository/Sonatype+OSS+Maven+Repository+Usage+Guide#SonatypeOSSMavenRepositoryUsageGuide-2.Signup) and [GPG signing key](http://www.dewinter.com/gnupg_howto/english/GPGMiniHowto-3.html#ss3.1)
3. Create a `gradle.properties` file containing your signing key-ring location and ID and yoursonatype username and password. For example:

    ```
    > cat /home/myusername/.gradle/gradle.properties
    signing.keyId=BAADF00D
    signing.secretKeyRingFile=/home/myusername/.gnupg/secring.gpg
    sonatypeUsername=myusername
    sonatypePassword=p455w0rd
    ```

4. Run `./gradlew uploadArchives`
5. When prompted, enter the passphrase for your GPG key

This should upload your archives to the Sonatype repository.

## TODO

* Add detailed instructions for deploying SNAPSHOT vs. release builds.

## Keep This Document Alive!

If you run into any issues following these instructions, please update this document with clarifications or workarounds.
