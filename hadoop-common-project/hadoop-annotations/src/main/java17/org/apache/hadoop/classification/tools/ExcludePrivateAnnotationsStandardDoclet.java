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

import jdk.javadoc.doclet.DocletEnvironment;
import jdk.javadoc.doclet.Reporter;
import javax.lang.model.SourceVersion;
import jdk.javadoc.doclet.StandardDoclet;

/**
 * A <a href="http://java.sun.com/javase/6/docs/jdk/api/javadoc/doclet/">Doclet</a>
 * for excluding elements that are annotated with
 * {@link org.apache.hadoop.classification.InterfaceAudience.Private} or
 * {@link org.apache.hadoop.classification.InterfaceAudience.LimitedPrivate}.
 * It delegates to the Standard Doclet, and takes the same options.
 */
public class ExcludePrivateAnnotationsStandardDoclet {
  
  public static SourceVersion languageVersion() {
    return SourceVersion.RELEASE_17;
  }
  
  public static boolean start(DocletEnvironment root) {
    System.out.println(
        ExcludePrivateAnnotationsStandardDoclet.class.getSimpleName());
    DocletEnvironment excludedDoc = RootDocProcessor.process(root);
    if (excludedDoc.getSpecifiedElements().isEmpty()) {
      return true;
    }
    return new StandardDoclet().run(excludedDoc);
  }
  
  public static int optionLength(String option) {
    Integer length = StabilityOptions.optionLength(option);
    if (length != null) {
      return length;
    }
    for (jdk.javadoc.doclet.Doclet.Option o :
        new StandardDoclet().getSupportedOptions()) {
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
}
