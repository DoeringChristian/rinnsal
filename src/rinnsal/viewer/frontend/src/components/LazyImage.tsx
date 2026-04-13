import { useEffect, useRef, useState } from "react";

interface LazyImageProps {
  src: string;
  alt: string;
  className?: string;
  /** Distance from viewport (px) at which to start fetching. */
  rootMargin?: string;
}

/**
 * Lazy <img> — starts fetching only when the element is close to the
 * viewport. Prevents the browser from prefetching every figure in a
 * long flow on page load. Uses the standard IntersectionObserver; the
 * browser's built-in ``loading="lazy"`` is inconsistent across engines
 * and doesn't help when the element is already in the initial DOM.
 */
export default function LazyImage({
  src,
  alt,
  className,
  rootMargin = "400px",
}: LazyImageProps) {
  const ref = useRef<HTMLImageElement | null>(null);
  const [visible, setVisible] = useState(false);

  useEffect(() => {
    if (visible) return;
    const el = ref.current;
    if (!el) return;
    const io = new IntersectionObserver(
      (entries) => {
        for (const entry of entries) {
          if (entry.isIntersecting) {
            setVisible(true);
            io.disconnect();
            break;
          }
        }
      },
      { rootMargin },
    );
    io.observe(el);
    return () => io.disconnect();
  }, [visible, rootMargin]);

  return (
    <img
      ref={ref}
      src={visible ? src : undefined}
      alt={alt}
      className={className}
      loading="lazy"
    />
  );
}
