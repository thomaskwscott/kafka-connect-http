package uk.co.threefi.connect.http.sink;


import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

public class RestHelper extends HttpServlet {

    private Server server;
    private static int port;
    private static List<RequestInfo> capturedRequests = new ArrayList<RequestInfo>();

    public void start() throws Exception {
        //flushCapturedRequests();
        server = new Server(0);
        ServerConnector connector = new ServerConnector(server);
        ServletContextHandler handler = new ServletContextHandler();
        ServletHolder testServ = new ServletHolder("test", RestHelper.class);
        testServ.setInitParameter("resourceBase",System.getProperty("user.dir"));
        testServ.setInitParameter("dirAllowed","true");
        handler.addServlet(testServ,"/test");
        handler.addServlet(testServ,"/someTopic");
        handler.addServlet(testServ,"/someKey");



        server.setHandler(handler);
        server.setConnectors(new Connector[]{connector});

        server.start();

        port = server.getURI().getPort();

    }

    public void stop() throws Exception {
        server.stop();
    }

    public int getPort() {
        return port;
    }

    protected void doGet(
            HttpServletRequest request,
            HttpServletResponse response)
            throws ServletException, IOException {

        capturedRequests.add(getRequestInfo(request));

        response.setContentType("application/json");
        response.setStatus(HttpServletResponse.SC_OK);
        response.getWriter().println("{ \"status\": \"ok\"}");
    }

    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {

        capturedRequests.add(getRequestInfo(request));

        response.setContentType("application/json");
        response.setStatus(HttpServletResponse.SC_OK);
        response.getWriter().println("{ \"status\": \"ok\"}");
    }

    protected void doPut(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {

        capturedRequests.add(getRequestInfo(request));

        response.setContentType("application/json");
        response.setStatus(HttpServletResponse.SC_OK);
        response.getWriter().println("{ \"status\": \"ok\"}");
    }

    protected void doDelete(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {

        capturedRequests.add(getRequestInfo(request));

        response.setContentType("application/json");
        response.setStatus(HttpServletResponse.SC_OK);
        response.getWriter().println("{ \"status\": \"ok\"}");
    }

    private RequestInfo getRequestInfo(HttpServletRequest request) throws IOException {
        RequestInfo requestInfo = new RequestInfo();


        // Read from request
        StringBuilder buffer = new StringBuilder();
        BufferedReader reader = request.getReader();
        String line;
        while ((line = reader.readLine()) != null) {
            buffer.append(line);
        }
        reader.close();
        requestInfo.setBody(buffer.toString());
        requestInfo.setUrl(request.getRequestURI());
        requestInfo.setMethod(request.getMethod());
        requestInfo.setTimeStamp(System.currentTimeMillis());
        Enumeration<String> headerNames = request.getHeaderNames();
        List<String> headers = new ArrayList<>();
        while (headerNames.hasMoreElements()) {
            String headerName = headerNames.nextElement();
            Enumeration<String> requestHeaders = request.getHeaders(headerName);
            while (requestHeaders.hasMoreElements()) {
                String headerValue = requestHeaders.nextElement();
                headers.add(headerName + ":" + headerValue);
            }
        }
        requestInfo.setHeaders(headers);
        return requestInfo;
    }

    public List<RequestInfo> getCapturedRequests() {
        return capturedRequests;
    }

    public void flushCapturedRequests() {
        capturedRequests.clear();
    }



}
