package cloudant.com.androidtest;

import android.os.Environment;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Text;

import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

/**
 * Created by rhys on 29/08/2014.
 */
public class JUnitXMLFormatter implements XMLConstants {

    private File outputFile = null;
    private Document xmlDocument = null;
    private DocumentBuilder builder = null;

    public JUnitXMLFormatter(){
        this.outputFile = new File(Environment.getExternalStorageDirectory(),BuildConfig.testOutputFile);
        //create the needed buffers

        if(outputFile.isDirectory()){
            //default to a file under a dir
            outputFile = new File(outputFile,"TestResults.xml");
        }

        try {
            BufferedOutputStream buffer = new BufferedOutputStream(new FileOutputStream(outputFile));
            builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
        } catch(ParserConfigurationException e){
            //do nothing
        } catch (FileNotFoundException e){
            // TODO should do something about that
        }
    }


    public void outputResults(List<TestResults> completedTests) throws IOException, TransformerException {
        xmlDocument = builder.newDocument();
        Element root = xmlDocument.createElement(TESTSUITE); //don't have the name for the test suite right?
        root.setAttribute(ATTR_NAME,"UNKOWN");
        root.setAttribute(ATTR_TESTS,String.valueOf(completedTests.size()));
        xmlDocument.appendChild(root);
        //need to work ouf the failure count

        int failureCount = 0;

        //so now lets actually process the tests yay

        for(TestResults test:completedTests){
            //create test element
            Element testElement = xmlDocument.createElement(TESTCASE);
            testElement.setAttribute(ATTR_NAME,test.methodName());
            testElement.setAttribute(ATTR_CLASSNAME,test.className());
            testElement.setAttribute(ATTR_TIME,String.valueOf(test.executionTime()));
            root.appendChild(testElement);

            if(test.hasFailed()){
                failureCount++;
                Element failure = xmlDocument.createElement(FAILURE); //call them all failures will distingush later maybe
                if(test.failureMessage() != null) {
                    failure.setAttribute(ATTR_MESSAGE, test.failureMessage());
                }

                failure.setAttribute(ATTR_TYPE,test.exceptionType());
                Text trace = xmlDocument.createTextNode(test.exceptionStack());

                //append failure information
                failure.appendChild(trace);
                testElement.appendChild(failure);
            }
        }

        root.setAttribute(ATTR_FAILURES,String.valueOf(failureCount));

        TransformerFactory transformerFactory = TransformerFactory.newInstance();
        Transformer transformer = transformerFactory.newTransformer();
        DOMSource source = new DOMSource(xmlDocument);

        StreamResult sr = new StreamResult(new BufferedWriter(new FileWriter(outputFile)));
        transformer.transform(source,sr);
        sr.getWriter().close(); //TODO close properly even on error
    }

}
