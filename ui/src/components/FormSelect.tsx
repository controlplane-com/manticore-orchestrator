import { Fragment, useState } from 'react';
import { Listbox, Transition } from '@headlessui/react';
import { ChevronDownIcon, CheckIcon } from '@heroicons/react/20/solid';

interface FormSelectProps {
  label?: string;
  error?: string;
  helpText?: string;
  required?: boolean;
  value: string;
  onChange: (value: string) => void;
  options: Array<{ value: string; label: string }>;
  disabled?: boolean;
  className?: string;
  placeholder?: string;
  showSearch?: boolean;
}

export const FormSelect = ({
  label,
  error,
  helpText,
  required,
  value,
  onChange,
  options,
  disabled,
  className,
  placeholder = 'Select...',
  showSearch = false
}: FormSelectProps) => {
  const [searchQuery, setSearchQuery] = useState('');
  const selectedOption = options.find(opt => opt.value === value);

  const filteredOptions = showSearch
    ? options.filter(option =>
        option.label.toLowerCase().includes(searchQuery.toLowerCase())
      )
    : options;

  return (
    <div className={className}>
      <Listbox value={value} onChange={onChange} disabled={disabled}>
        {label && (
          <Listbox.Label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
            {label}
            {required && <span className="text-red-500 ml-1">*</span>}
          </Listbox.Label>
        )}

        <div className="relative">
          <Listbox.Button className={`relative w-full pl-3 pr-10 py-2 text-left border rounded-lg focus:ring-2 focus:ring-cpln-cyan focus:border-transparent ${
            error ? 'border-red-300' : 'border-gray-300 dark:border-stone-600'
          } ${disabled ? 'bg-gray-50 dark:bg-stone-900 text-gray-500 dark:text-gray-400' : 'bg-white dark:bg-stone-800 text-gray-900 dark:text-gray-100'}`}>
            <span className="block truncate">{selectedOption?.label || placeholder}</span>
            <span className="pointer-events-none absolute inset-y-0 right-0 flex items-center pr-3">
              <ChevronDownIcon className="h-5 w-5 text-gray-400" aria-hidden="true" />
            </span>
          </Listbox.Button>

          <Transition
            as={Fragment}
            leave="transition ease-in duration-100"
            leaveFrom="opacity-100"
            leaveTo="opacity-0"
            afterLeave={() => setSearchQuery('')}
          >
            <Listbox.Options
              anchor="bottom start"
              className="group z-10 max-h-60 w-[var(--button-width)] overflow-auto rounded-lg shadow-lg ring-1 ring-black ring-opacity-5 focus:outline-none bg-white dark:bg-stone-800"
            >
              {showSearch && (
                <div className="sticky top-0 bg-white dark:bg-stone-800 border-b border-gray-200 dark:border-stone-700 p-2">
                  <input
                    type="text"
                    className="w-full px-3 py-2 border border-gray-300 dark:border-stone-600 rounded-lg bg-white dark:bg-stone-700 text-gray-900 dark:text-gray-100 focus:ring-2 focus:ring-cpln-cyan focus:border-transparent"
                    placeholder="Search..."
                    value={searchQuery}
                    onChange={(e) => setSearchQuery(e.target.value)}
                    onClick={(e) => e.stopPropagation()}
                  />
                </div>
              )}
              {filteredOptions.length === 0 ? (
                <div className="px-3 py-2 text-sm text-gray-500 dark:text-gray-400">No options found</div>
              ) : (
                filteredOptions.map((option) => (
                  <Listbox.Option
                    key={option.value}
                    value={option.value}
                    className={({ active }) =>
                      `relative cursor-pointer select-none py-2 pl-10 pr-4 first:group-data-[side=bottom]:rounded-t-lg last:group-data-[side=bottom]:rounded-b-lg first:group-data-[side=top]:rounded-b-lg last:group-data-[side=top]:rounded-t-lg ${
                        active ? 'bg-cpln-cyan text-white' : 'bg-white dark:bg-stone-800 text-gray-900 dark:text-gray-100'
                      }`
                    }
                  >
                    {({ selected, active }) => (
                      <>
                        <span className={`block truncate ${selected ? 'font-medium' : 'font-normal'}`}>
                          {option.label}
                        </span>
                        {selected && (
                          <span className={`absolute inset-y-0 left-0 flex items-center pl-3 ${
                            active ? 'text-white' : 'text-cpln-cyan'
                          }`}>
                            <CheckIcon className="h-5 w-5" aria-hidden="true" />
                          </span>
                        )}
                      </>
                    )}
                  </Listbox.Option>
                ))
              )}
            </Listbox.Options>
          </Transition>
        </div>
      </Listbox>

      {helpText && !error && <p className="mt-1 text-sm text-gray-500 dark:text-gray-400">{helpText}</p>}
      {error && <p className="mt-1 text-sm text-red-600">{error}</p>}
    </div>
  );
};
