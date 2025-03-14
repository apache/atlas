import { useState, useEffect } from "react";
import { useNavigate } from "react-router-dom";

const ErrorBoundary: any = ({ children }: any) => {
  const navigate = useNavigate();
  const [hasError, setHasError] = useState(false);

  // Use error boundary pattern with try/catch
  useEffect(() => {
    const handleUncaughtErrors = (event: any) => {
      setHasError(true);
      event.preventDefault(); // Prevent the default error handling
      navigate("/"); // Redirect to the landing page
    };

    window.addEventListener("error", handleUncaughtErrors);

    return () => {
      window.removeEventListener("error", handleUncaughtErrors);
    };
  }, [navigate]);

  if (hasError) {
    return null; // You can return a custom fallback UI here if needed
  }

  return { children };
};

export default ErrorBoundary;
