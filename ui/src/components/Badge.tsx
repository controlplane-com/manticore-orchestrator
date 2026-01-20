interface BadgeProps {
  children: React.ReactNode;
  variant?: 'success' | 'warning' | 'error' | 'info' | 'default' | 'orange' | 'cyan' | 'muted';
  size?: 'sm' | 'md';
  className?: string;
}

export const Badge = ({ children, variant = 'default', size = 'md', className }: BadgeProps) => {
  const variantClasses = {
    success: 'bg-green-100 dark:bg-green-950 text-green-800 dark:text-green-300 border border-transparent dark:border-green-500',
    warning: 'bg-yellow-100 dark:bg-yellow-950 text-yellow-800 dark:text-yellow-300 border border-transparent dark:border-yellow-500',
    error: 'bg-red-100 dark:bg-red-950 text-red-800 dark:text-red-300 border border-transparent dark:border-red-500',
    info: 'bg-blue-100 dark:bg-blue-950 text-blue-800 dark:text-blue-300 border border-transparent dark:border-blue-500',
    default: 'bg-gray-100 dark:bg-gray-800 text-gray-800 dark:text-gray-300 border border-transparent dark:border-gray-600',
    orange: 'bg-orange-100 dark:bg-orange-950 text-orange-800 dark:text-orange-300 border border-transparent dark:border-orange-500',
    cyan: 'bg-white dark:bg-blue-950 text-cpln-cyan dark:text-blue-300 border border-cpln-cyan dark:border-blue-500',
    muted: 'bg-gray-200 dark:bg-gray-800 text-gray-500 dark:text-gray-400 border border-transparent',
  };

  const sizeClasses = {
    sm: 'px-2 py-0.5 text-xs',
    md: 'px-2.5 py-0.5 text-sm',
  };

  return (
    <span
      className={`inline-flex items-center rounded-full font-medium ${variantClasses[variant]} ${sizeClasses[size]} ${className || ''}`}
    >
      {children}
    </span>
  );
};
