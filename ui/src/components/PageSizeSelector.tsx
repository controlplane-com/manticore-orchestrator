import React from 'react';
import { FormSelect } from './FormSelect';

interface PageSizeSelectorProps {
  pageSize: number;
  onPageSizeChange: (size: number) => void;
  totalItems: number;
  className?: string;
}

export const PageSizeSelector: React.FC<PageSizeSelectorProps> = ({
  pageSize,
  onPageSizeChange,
  totalItems,
  className = '',
}) => {
  const options = [
    { value: '25', label: '25' },
    { value: '50', label: '50' },
    { value: '100', label: '100' },
    { value: 'all', label: 'All' },
  ];

  // Display "All" in the select if pageSize is greater than total items or very large
  const displayValue = pageSize >= totalItems || pageSize >= 1000 ? 'all' : String(pageSize);

  return (
    <div className={`flex items-center gap-2 ${className}`}>
      <label className="text-sm text-gray-700 dark:text-gray-300">
        Page Size:
      </label>
      <FormSelect
        value={displayValue}
        onChange={(value) => {
          // For "All", pass a very large number or the total items count
          if (value === 'all') {
            onPageSizeChange(Math.max(totalItems, 10000));
          } else {
            onPageSizeChange(parseInt(value, 10));
          }
        }}
        options={options}
        className="w-24"
      />
    </div>
  );
};
