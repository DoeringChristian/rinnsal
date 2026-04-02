import { useState, useEffect, useRef } from "react";

interface LazyRenderProps {
  height?: number;
  children: React.ReactNode;
}

/**
 * Only renders children when the component is in or near the viewport.
 * Shows a placeholder of the given height when off-screen.
 */
export function LazyRender({ height = 350, children }: LazyRenderProps) {
  const [visible, setVisible] = useState(false);
  const ref = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const el = ref.current;
    if (!el) return;

    const observer = new IntersectionObserver(
      ([entry]) => {
        if (entry.isIntersecting) {
          setVisible(true);
        }
      },
      { rootMargin: "200px" }, // Start rendering 200px before visible
    );

    observer.observe(el);
    return () => observer.disconnect();
  }, []);

  if (!visible) {
    return (
      <div
        ref={ref}
        style={{ minHeight: height }}
        className="bg-gray-50 rounded-lg border border-gray-100 animate-pulse"
      />
    );
  }

  return <div ref={ref}>{children}</div>;
}
