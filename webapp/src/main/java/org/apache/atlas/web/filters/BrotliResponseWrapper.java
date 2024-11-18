package org.apache.atlas.web.filters;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpServletResponseWrapper;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;

public class BrotliResponseWrapper extends HttpServletResponseWrapper {

    private final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    private ServletOutputStream servletOutputStream;
    private PrintWriter writer;

    public BrotliResponseWrapper(HttpServletResponse response) {
        super(response);
    }

    @Override
    public ServletOutputStream getOutputStream() throws IOException {
        if (servletOutputStream == null) {
            servletOutputStream = new ServletOutputStream() {
                @Override
                public void write(int b) {
                    outputStream.write(b);
                }

                @Override
                public boolean isReady() {
                    return true;
                }

                @Override
                public void setWriteListener(javax.servlet.WriteListener listener) {
                    // No-op for this example
                }
            };
        }
        return servletOutputStream;
    }

    @Override
    public PrintWriter getWriter() throws IOException {
        if (writer == null) {
            writer = new PrintWriter(outputStream);
        }
        return writer;
    }

    public byte[] getOutputStreamData() {
        return outputStream.toByteArray();
    }
}
