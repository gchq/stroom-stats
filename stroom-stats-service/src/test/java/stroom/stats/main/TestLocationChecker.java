/*
 * Copyright 2017 Crown Copyright
 *
 * This library is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 2.1 of the License, or (at your option)
 * any later version.
 *
 * This library is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details.
 *
 * You should have received a copy of the GNU Lesser General Public License along
 * with this library; if not, write to the Free Software Foundation, Inc., 59
 * Temple Place, Suite 330, Boston, MA 02111-1307 USA
 *
 *
 */

package stroom.stats.main;

import com.google.common.base.Strings;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * Main method utility to find test classes in different module or package to the tested class.
 * Assume test class is either TestX.java or XTest.java where X is the name of the class under
 * test
 */
public class TestLocationChecker {

    private static final Path PROJECT_ROOT_PATH = Paths.get(".");

    private static final boolean INCLUDE_CLASSES_WITH_NO_TESTS = false;

    private final Map<String, List<ClassWrapper>> testClassBaseNameMap = new HashMap<>();

    public static void main(String[] args) throws IOException {
        new TestLocationChecker().run();
    }

    private void processJavaFile(Path javaFilePath) {
        ClassWrapper javaClass = new ClassWrapper(javaFilePath);

        List<ClassWrapper> testClasses = testClassBaseNameMap.getOrDefault(javaClass.getBaseName(), new ArrayList<>());

//            System.out.println(javaClass);
//            System.out.println("StroomStatsHbaseClientModule [" + javaClass.getModule() + "]");

        if (!testClasses.isEmpty()) {
            testClasses.forEach(testClass -> {
//                    System.out.println(javaClass.getModule() + "/" + javaClass.getFullyQualifiedName() +
//                            " - " + testClass.getModule() + "/" + testClass.getFullyQualifiedName());

                String reason = "";
                if (!javaClass.isSameModule(testClass)) {
                    reason += "DIFF_MOD";
                }

                if (!javaClass.isSamePackage(testClass)) {
                    reason += ",DIFF_PKG";
                }
                if (!reason.isEmpty()) {
                    dumpClassPair(javaClass, testClass, reason.replaceFirst("^,", ""));
                }
            });
        } else {
            //Comment this line out if you don't care about classes with no test class
            if (INCLUDE_CLASSES_WITH_NO_TESTS) {
                dumpClassPair(javaClass, null, "NO_TEST");
            }
        }
    }

    private void run() throws IOException {

        System.out.println("PROJECT_ROOT_PATH: " + PROJECT_ROOT_PATH.toAbsolutePath().toString());

        cacheAllTests();

        performFileActionsInEachModule(filePath -> filePath.toString().contains("src/main/java"),
                this::processJavaFile,
                Paths.get("src", "main", "java"));
    }

    private void dumpClassPair(ClassWrapper javaClass, ClassWrapper testClass, String reason) {
        StringBuilder sb = new StringBuilder();
        sb.append(Strings.padEnd(reason, 20, ' '));
        sb.append(Strings.padEnd(javaClass.getModule(), 30, ' '));
        sb.append(Strings.padEnd(javaClass.getFullyQualifiedName(), 80, ' '));
        if (testClass != null) {
            sb.append(Strings.padEnd(testClass.getModule(), 30, ' '));
            sb.append(Strings.padEnd(testClass.getFullyQualifiedName(), 80, ' '));
        }
        System.out.println(sb.toString());
    }

    private void performFileActionsInEachModule(Predicate<Path> fileTest,
                                                Consumer<Path> fileAction,
                                                Path relativeSubPath) throws IOException {
        Consumer<Path> processModuleDir = (modulePath) -> {
//            System.out.println("Processing module [" + modulePath.getFileName().toString() + "]");
            try {
//                Path srcPath = Paths.get(modulePath.toString(), "src", "main", "java");
                Path srcPath = modulePath.resolve(relativeSubPath);
//                System.out.println("srcPath [" + srcPath + "]");

                if (Files.isDirectory(srcPath)) {
                    Files.walk(srcPath)
                            .parallel()
                            .filter(Files::isRegularFile)
                            .filter(fileTest)
                            .filter(path -> path.getFileName().toString().endsWith(".java"))
                            .forEach(fileAction);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        };

        Files.list(PROJECT_ROOT_PATH)
                .parallel()
                .filter(Files::isDirectory)
//                .filter(fileTest)
                .forEach(processModuleDir);

    }

    private void cacheAllTests() throws IOException {
        Consumer<Path> processTestFile = filePath -> {
            if (filePath.getFileName().toString().contains("Test")) {

                ClassWrapper testClass = new ClassWrapper(filePath);

//                System.out.println(testClass);

                testClassBaseNameMap.computeIfAbsent(testClass.getBaseName(), key -> new ArrayList<>())
                        .add(testClass);
            }
        };

        performFileActionsInEachModule(filePath -> filePath.toString().contains("src/test/java"),
                processTestFile,
                Paths.get("src", "test", "java"));

        System.out.println("Cached test count: " + testClassBaseNameMap.size());

    }

    private static final class ClassWrapper {
        private final String baseName;
        private final String fullName;
        private final Path path;
        private String packageName;
        private String module;

        ClassWrapper(final Path path) {
            this.packageName = null;
            try (Stream<String> stream = Files.lines(path)) {
                this.packageName = stream
//                            .peek(line -> System.out.println("line: [" + line + "]"))
                        .filter(line -> line.matches("^package .*"))
                        .findFirst().orElseThrow(() -> new RuntimeException(String.format("Class %s has no package string", path.toString())))
                        .replaceAll("^package ","")
                        .replaceAll(";","");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            this.fullName = path.getFileName()
                    .toString()
                    .replaceAll("\\.java", "");
            this.baseName = fullName.replaceAll("^Test", "")
                    .replaceAll("Test$", "");
            this.path = path;
            this.module = path.subpath(1,2).toString();
        }

        String getBaseName() {
            return baseName;
        }

        String getModule() {
            return module;
        }

        String getFullyQualifiedName() {
            return packageName + "." + fullName;
        }

        boolean isSameModule(ClassWrapper other) {
            return this.module.equals(other.module);
        }

        boolean isSamePackage(ClassWrapper other) {
            return this.packageName.equals(other.packageName);
        }

        @Override
        public String toString() {
            return "ClassWrapper{" +
                    "baseName='" + baseName + '\'' +
                    ", fullName='" + fullName + '\'' +
                    ", path=" + path +
                    ", packageName='" + packageName + '\'' +
                    '}';
        }
    }
}
