package org.smcql.codegen.smc;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.logging.Logger;
import java.io.FilenameFilter;

import javax.tools.Diagnostic;
import javax.tools.DiagnosticListener;
import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.SimpleJavaFileObject;
import javax.tools.StandardJavaFileManager;
import javax.tools.ToolProvider;

import org.apache.commons.lang3.StringUtils;
import org.smcql.config.SystemConfiguration;
import org.smcql.util.ClassPathUpdater;
import org.smcql.util.Utilities;

import com.oblivm.backend.flexsc.CompEnv;
import com.oblivm.backend.gc.GCSignal;
import com.oblivm.backend.lang.inter.ISecureRunnable;
import com.oblivm.backend.oram.SecureArray;
import com.oblivm.compiler.cmd.Cmd;

/**
 * Dynamic java class compiler and executer  <br>
 * Demonstrate how to compile dynamic java source code, <br>
 * instantiate instance of the class, and finally call method of the class <br>
 *
 * http://www.beyondlinux.com
 *
 * @author david 2011/07
 *
 */
public class DynamicCompiler
{
    /** where shall the compiled class be saved to (should exist already) */
    private static String classOutputFolder =  Utilities.getCodeGenTarget();

    public static class MyDiagnosticListener implements DiagnosticListener<JavaFileObject>
    {
        public void report(Diagnostic<? extends JavaFileObject> diagnostic)
        {

            System.out.println("Line Number->" + diagnostic.getLineNumber());
            System.out.println("code->" + diagnostic.getCode());
            System.out.println("Message->"
                               + diagnostic.getMessage(Locale.ENGLISH));
            System.out.println("Source->" + diagnostic.getSource());
            System.out.println(" ");
        }
    }

    /** java File Object represents an in-memory java source file <br>
     * so there is no need to put the source file on hard disk  **/
    public static class InMemoryJavaFileObject extends SimpleJavaFileObject
    {
        private String contents = null;

        public InMemoryJavaFileObject(String className, String contents) throws Exception
        {
            super(URI.create("string:///" + className.replace('.', '/')
                             + Kind.SOURCE.extension), Kind.SOURCE);
            this.contents = contents;
        }

        public CharSequence getCharContent(boolean ignoreEncodingErrors)
                throws IOException
        {
            return contents;
        }
    }


    @SuppressWarnings("unchecked")
	public static<T> ISecureRunnable<T> loadClass(String packageName, CompEnv<T> env, String jarFile) throws Exception {
    	System.out.println("Loading from " + jarFile + " looking for " + packageName);

    	String className = packageName + ".NoClass";

    	
    	ClassPathUpdater.add(jarFile);
    	File f = new File(jarFile);
		URL[] cp = {f.toURI().toURL()};
		@SuppressWarnings("resource")
		URLClassLoader urlcl = new URLClassLoader(cp);

		Class<?> cl = urlcl.loadClass(className);
		Constructor<?> ctor = cl.getConstructors()[0];
		return (ISecureRunnable<T>)ctor.newInstance(env);


    }
  
    
    @SuppressWarnings("unchecked")
	public static <T> ISecureRunnable<T> loadClass(String packageName, CompEnv<T> env) throws Exception {
    	String className = packageName + ".NoClass";

			File f = new File(Utilities.getCodeGenTarget());
			URL[] cp = {f.toURI().toURL()};
			@SuppressWarnings("resource")
			URLClassLoader urlcl = new URLClassLoader(cp);

			Class<?> cl = urlcl.loadClass(className);
			Constructor<?> ctor = cl.getConstructors()[0];
			return (ISecureRunnable<T>)ctor.newInstance(env);

	
    }
    

    @SuppressWarnings("unchecked")
	public static <T> ISecureRunnable<T> loadClass(String packageName, byte[] byteCode, CompEnv<T> env) throws Exception {
    	ByteArrayClassLoader loader = new ByteArrayClassLoader(packageName, byteCode, Thread.currentThread().getContextClassLoader());  	
    	Class<?> cl = loader.findClass(packageName);
    	Constructor<?> ctor = cl.getConstructors()[0];
    	ISecureRunnable<T> newInstance = (ISecureRunnable<T>)ctor.newInstance(env);
    	return newInstance;
    }

    /** compile your files by JavaCompiler */
    @SuppressWarnings("rawtypes")
	public static void compile(Iterable<? extends JavaFileObject> files) throws Exception
    {
        //get system compiler:
        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();

        // for compilation diagnostic message processing on compilation WARNING/ERROR
        MyDiagnosticListener c = new MyDiagnosticListener();
        StandardJavaFileManager fileManager = compiler.getStandardFileManager(c,
                                                                              Locale.ENGLISH,
                                                                              null);
        //specify classes output folder
        Iterable options = Arrays.asList("-d", classOutputFolder);
        @SuppressWarnings("unchecked")
		JavaCompiler.CompilationTask task = compiler.getTask(null, fileManager,
                                                             c, options, null,
                                                             files);
        Boolean result = task.call();
        if (result == false)
        {
        	throw new Exception("Compile failed!");
        }
    }
    
    public static void compileJava(String srcFile, String packageName) throws Exception {
		List<String> code = Utilities.readFile(srcFile);
		String smcCode = StringUtils.join(code.toArray(), "\n");
	
		JavaFileObject so = new InMemoryJavaFileObject(packageName, smcCode);
        Iterable<? extends JavaFileObject> files = Arrays.asList(so);
        DynamicCompiler.compile(files);

    }
    
    public static void compileJava(String[] srcFiles, String packageName, String path) throws Exception {
    	JavaFileObject[] sos = new JavaFileObject[srcFiles.length];
    	for (int i = 0; i < srcFiles.length; i++) {
    		String file = path + "/" + srcFiles[i];
	    	List<String> code = Utilities.readFile(file);
			String smcCode = StringUtils.join(code.toArray(), "\n");

		
			sos[i] = new InMemoryJavaFileObject(packageName + "." + srcFiles[i].replace(".java", ""), smcCode);
    	}
	    Iterable<? extends JavaFileObject> files = Arrays.asList(sos);
        DynamicCompiler.compile(files);
    }
    
    
    public static void compileOblivFromFile(String srcFile) throws Exception {
    	String dstPath = Utilities.getCodeGenTarget();

    	List<String> lines = Utilities.readFile(srcFile);
    	String firstLine = lines.get(0);

    	String[] packageTokens = firstLine.split(" ");
    	String packageName = packageTokens[1].substring(0, packageTokens[1].length() - 1);

    	
    	Cmd.compile(srcFile, dstPath);
    	
    	String javaPath = dstPath + "/" + packageName.replace('.', '/');

    	File dir = new File(javaPath);
    	File[] files = dir.listFiles(new FilenameFilter() {
    		  public boolean accept(File dir, String name) {
    		    return name.endsWith(".java");
    		  }
    		});
    	String[] fileNames = new String[files.length];
    	for (int i = 0; i < files.length; i ++) {
    		fileNames[i] = files[i].getName();
    	}
		
		DynamicCompiler.compileJava(fileNames, packageName, javaPath);
		
    }
    
    public static void compileOblivLang(String smcCode, String packageName) throws Exception {
    	String dstPath = Utilities.getCodeGenTarget() + "/";
		String srcFile = dstPath + "tmp.lcc";
		
		Utilities.writeFile(srcFile, smcCode);

		Cmd.compile(srcFile, dstPath);
		
		String javaPath = dstPath + packageName.replace('.', '/');
    	File dir = new File(javaPath);

    	File[] files = dir.listFiles(new FilenameFilter() {
    		  public boolean accept(File dir, String name) {
    		    return name.endsWith(".java");
    		  }
    		});
    	
    	String[] fileNames = new String[files.length];
    	for (int i = 0; i < files.length; i ++) {
    		fileNames[i] = files[i].getName();
    	}
		
		DynamicCompiler.compileJava(fileNames, packageName, javaPath);
		
		Path srcPath = Paths.get(srcFile);
		
		Files.deleteIfExists(srcPath);

    }
    

    public static void runGenerator(String className, String host, int port, SecureArray<GCSignal> input)  {
    }
    
    
    /** run class from the compiled byte code file by URLClassloader */
    public static void runIt(String className)
    {
        // Create a File object on the root of the directory
        // containing the class file
        File file = new File(classOutputFolder);

        try
        {
            // Convert File to a URL
            URL url = file.toURI().toURL(); // file:/classes/demo
            URL[] urls = new URL[] { url };

            // Create a new class loader with the directory
            ClassLoader loader = new URLClassLoader(urls);

            // Load in the class; Class.childclass should be located in
            // the directory file:/class/demo/
            Class thisClass = loader.loadClass(className);

            Class params[] = {};
            Object paramsObj[] = {};
            Object instance = thisClass.newInstance();
            @SuppressWarnings("unchecked")
			Method thisMethod = thisClass.getDeclaredMethod("testAdd", params);

            // run the testAdd() method on the instance:
            thisMethod.invoke(instance, paramsObj);
        }
        catch (MalformedURLException e)
        {
        }
        catch (ClassNotFoundException e)
        {
        }
        catch (Exception ex)
        {
            ex.printStackTrace();
        }
    }

    
       
}
