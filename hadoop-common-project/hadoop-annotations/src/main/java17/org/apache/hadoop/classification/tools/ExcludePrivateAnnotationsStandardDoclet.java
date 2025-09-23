/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.classification.tools;

import jdk.javadoc.doclet.Doclet;
import jdk.javadoc.doclet.DocletEnvironment;
import jdk.javadoc.doclet.Reporter;
import javax.lang.model.SourceVersion;
import jdk.javadoc.doclet.StandardDoclet;

import java.util.Locale;
import java.util.Set;

/**
 * A <a href="http://java.sun.com/javase/6/docs/jdk/api/javadoc/doclet/">Doclet</a>
 * for excluding elements that are annotated with
 * {@link org.apache.hadoop.classification.InterfaceAudience.Private} or
 * {@link org.apache.hadoop.classification.InterfaceAudience.LimitedPrivate}.
 * It delegates to the Standard Doclet, and takes the same options.
 */
public class ExcludePrivateAnnotationsStandardDoclet implements Doclet {

  private final StandardDoclet delegate = new StandardDoclet();
  private Reporter reporter;
  private Locale locale;

  // === 你已有的静态方法：原样保留（旧调用方不炸） ===
  public static SourceVersion languageVersion() {
    return SourceVersion.RELEASE_17;
  }

  public static boolean start(DocletEnvironment root) {
    System.out.println(ExcludePrivateAnnotationsStandardDoclet.class.getSimpleName());
    if (root.getSpecifiedElements().isEmpty()) {
      return true;
    }
    return new StandardDoclet().run(root);
  }

  public static int optionLength(String option) {
    Integer length = StabilityOptions.optionLength(option);
    if (length != null) return length;
    for (Doclet.Option o : new StandardDoclet().getSupportedOptions()) {
      for (String name : o.getNames()) {
        if (name.equals(option)) {
          return o.getArgumentCount() + 1;
        }
      }
    }
    return 0;
  }

  public static boolean validOptions(String[][] options, Reporter reporter) {
    StabilityOptions.validOptions(options, reporter);
    return true;
  }

  @Override
  public void init(Locale locale, Reporter reporter) {
    this.locale = locale;
    this.reporter = reporter;
    delegate.init(locale, reporter);
  }

  @Override
  public String getName() {
    return getClass().getSimpleName();
  }

  @Override
  public Set<Option> getSupportedOptions() {
    Set<Option> s = new java.util.HashSet<>(delegate.getSupportedOptions());
    s.add(new Option() {
      @Override
      public int getArgumentCount() {
        return 0;
      }

      @Override
      public String getDescription() {
        return "";
      }

      @Override
      public Kind getKind() {
        return Kind.OTHER;
      }

      @Override
      public java.util.List<String> getNames() {
        return java.util.Collections.singletonList("-unstable");
      }

      @Override
      public String getParameters() {
        return "";
      }

      @Override
      public boolean process(String opt, java.util.List<String> args) {
        // 如需影响过滤逻辑，在此设置你的开关
        return true;
      }
    });
    s.add(new Option() {
      @Override
      public int getArgumentCount() {
        return 0;
      }

      @Override
      public String getDescription() {
        return "";
      }

      @Override
      public Kind getKind() {
        return Kind.OTHER;
      }

      @Override
      public java.util.List<String> getNames() {
        return java.util.Collections.singletonList("-evolving");
      }

      @Override
      public String getParameters() {
        return "";
      }

      @Override
      public boolean process(String opt, java.util.List<String> args) {
        return true;
      }
    });
    return s;
  }

  @Override
  public SourceVersion getSupportedSourceVersion() {
    return SourceVersion.RELEASE_17;
  }

  @Override
  public boolean run(DocletEnvironment environment) {
    DocletEnvironment excluded = RootDocProcessor.process(environment);
    if (environment.getSpecifiedElements().isEmpty()) {
      return true;
    }
    return delegate.run(environment);
  }
}
