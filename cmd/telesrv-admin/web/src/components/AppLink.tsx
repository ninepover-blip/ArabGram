import type { ReactNode } from "react";
import type { Navigate } from "../routing";

export function AppLink({
  href,
  navigate,
  className,
  children
}: {
  href: string;
  navigate: Navigate;
  className?: string;
  children: ReactNode;
}) {
  return (
    <a
      className={className}
      href={href}
      onClick={(event) => {
        event.preventDefault();
        navigate(href);
      }}
    >
      {children}
    </a>
  );
}
