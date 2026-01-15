import { ReactNode } from 'react';

interface CardProps {
  children: ReactNode;
  className?: string;
}

export const Card = ({ children, className = '' }: CardProps) => {
  return (
    <div className={`bg-white dark:bg-stone-800 rounded-lg border border-gray-200 dark:border-stone-700 ${className}`}>
      {children}
    </div>
  );
};
